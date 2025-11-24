# utils.py
import os
import io
import json
from pathlib import Path
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


def _build_credentials():
    """
    Carrega credenciais do arquivo .env.
    Não tenta acessar st.secrets (evita erro local).
    """
    raw = os.getenv("GOOGLE_CREDENTIALS")

    if not raw:
        raise RuntimeError(
            "❌ GOOGLE_CREDENTIALS não encontrada. "
            "Configure no arquivo .env na raiz do projeto."
        )

    raw = raw.strip()
    if raw.startswith('"""') and raw.endswith('"""'):
        raw = raw[3:-3]
    elif raw.startswith('"') and raw.endswith('"'):
        raw = raw[1:-1]

    try:
        info = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"❌ Erro ao interpretar GOOGLE_CREDENTIALS como JSON: {e}")

    try:
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
        return creds
    except Exception as e:
        raise RuntimeError(f"❌ Erro ao criar objeto de credencial: {e}")


def get_services():
    """
    Retorna os serviços do Google Drive e Sheets.
    As credenciais são criadas somente quando necessário.
    """
    creds = _build_credentials()
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


def list_gsheets_in_folder(folder_id: str):
    """
    Lista arquivos XLSX em uma pasta do Drive.
    Retorna lista de dicts: [{id: ..., name: ...}, ...]
    """
    drive_service, _ = get_services()
    query = (
        f"'{folder_id}' in parents "
        "and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
        "and trashed = false"
    )

    results = (
        drive_service.files()
        .list(q=query, fields="files(id, name)", pageSize=1000)
        .execute()
    )
    return results.get("files", [])


def read_gsheet_to_df(file_id: str) -> pd.DataFrame:
    """
    Baixa o XLSX do Drive e retorna DataFrame.
    Testa diferentes configurações de leitura para encontrar a correta.
    """
    drive_service, _ = get_services()
    data = drive_service.files().get_media(fileId=file_id).execute()
    bio = io.BytesIO(data)
    
    # TESTE 1: Ler tudo sem header para ver a estrutura
    print(f"\n{'='*60}")
    print(f"[DEBUG] Analisando arquivo {file_id}")
    print(f"{'='*60}")
    
    bio.seek(0)  # Volta ao início
    df_raw = pd.read_excel(bio, header=None)
    print(f"[DEBUG] TESTE 1 - Leitura RAW (sem header):")
    print(f"  Total de linhas: {len(df_raw)}")
    print(f"  Primeiras 5 linhas completas:")
    print(df_raw.head(5))
    print(f"\n  Linha 0: {list(df_raw.iloc[0])}")
    print(f"  Linha 1: {list(df_raw.iloc[1])}")
    if len(df_raw) > 2:
        print(f"  Linha 2: {list(df_raw.iloc[2])}")
    
    # TESTE 2: Ler com header=1
    bio.seek(0)
    df_h1 = pd.read_excel(bio, header=1)
    print(f"\n[DEBUG] TESTE 2 - Leitura com header=1:")
    print(f"  Colunas: {list(df_h1.columns)}")
    print(f"  Total de linhas: {len(df_h1)}")
    print(f"  Primeiras 5 linhas:")
    print(df_h1.head(5))
    
    # TESTE 3: Quantas linhas têm valores nas colunas principais?
    if 'Qtd. WMS' in df_h1.columns or 'B' in df_raw.columns:
        col_wms = 'Qtd. WMS' if 'Qtd. WMS' in df_h1.columns else 1  # Coluna B = índice 1
        
        if isinstance(col_wms, str):
            linhas_com_dados = df_h1[col_wms].notna().sum()
            print(f"\n[DEBUG] TESTE 3 - Coluna '{col_wms}' tem {linhas_com_dados} valores não-nulos")
            print(f"  Valores únicos (primeiros 20): {df_h1[col_wms].dropna().unique()[:20]}")
        else:
            linhas_com_dados = df_raw[col_wms].notna().sum()
            print(f"\n[DEBUG] TESTE 3 - Coluna índice {col_wms} tem {linhas_com_dados} valores não-nulos")
    
    # Escolher a melhor leitura
    df = df_h1.copy()
    
    # Remove APENAS linhas onde TODAS as colunas são NaN
    linhas_antes = len(df)
    df = df.dropna(how='all')
    linhas_depois = len(df)
    
    print(f"\n[DEBUG] RESULTADO FINAL:")
    print(f"  Linhas antes do dropna: {linhas_antes}")
    print(f"  Linhas removidas: {linhas_antes - linhas_depois}")
    print(f"  Linhas finais: {linhas_depois}")
    print(f"  Colunas: {list(df.columns)}")
    print(f"{'='*60}\n")
    
    return df


def write_output(df: pd.DataFrame, destino: str = "sheets"):
    """
    Salva CSV local e, se solicitado, atualiza a planilha do Google Sheets.
    """

    # 1) Salvar histórico local
    hist_path = os.getenv("HIST_SOURCE", "_outputs/base_inventario_12meses.csv")
    Path(hist_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(hist_path, index=False, encoding="utf-8-sig")

    # Se não for pra escrever no Sheets, finalize aqui
    if destino != "sheets":
        return

    # 2) Obter ID da planilha
    sheet_id = os.getenv("SHEET_OUTPUT_ID")
    if not sheet_id:
        raise RuntimeError(
            "❌ SHEET_OUTPUT_ID não definido. Configure no .env ou no Secrets."
        )

    # 3) Fazer uma cópia para não alterar o original
    df_out = df.copy()

    # 4) Converter colunas de data/hora para string (JSON serializable)
    for col in df_out.columns:
        if pd.api.types.is_datetime64_any_dtype(df_out[col]):
            df_out[col] = df_out[col].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 5) Substituir NaN/NaT por strings vazias
    df_out = df_out.astype(object).where(pd.notna(df_out), "")

    # 6) Preparar valores para o Google Sheets
    values = [df_out.columns.tolist()] + df_out.values.tolist()

    # 7) Obter serviço do Sheets
    _, sheets_service = get_services()

    # 8) Limpar planilha
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id, range="A:ZZ"
    ).execute()

    # 9) Escrever dados
    body = {"values": values}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="A1",
        valueInputOption="RAW",
        body=body,
    ).execute()