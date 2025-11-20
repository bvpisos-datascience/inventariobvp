# utils.py
import os
import io
import json
from pathlib import Path
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build

# ----------------------------------------
# Escopos usados pelas APIs
# ----------------------------------------
SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]


# ========================================
# 1) MONTAGEM DE CREDENCIAIS (APENAS DO .env — FUNCIONA LOCALMENTE)
# ========================================

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

    # Remove possíveis aspas extras (caso copie errado)
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


# Função para obter serviços sob demanda
def get_services():
    """
    Retorna os serviços do Google Drive e Sheets.
    As credenciais são criadas somente quando necessário.
    """
    creds = _build_credentials()
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)
    return drive_service, sheets_service


# ========================================
# 2) Funções utilitárias
# ========================================

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
    """
    drive_service, _ = get_services()
    data = drive_service.files().get_media(fileId=file_id).execute()
    bio = io.BytesIO(data)
    df = pd.read_excel(bio)
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
            "❌ SHEET_OUTPUT_ID não definido. Configure no .env."
        )

    # Transformação do dataframe
    df_out = df.copy().astype(object).where(pd.notna(df), "")

    values = [list(df_out.columns)] + df_out.values.tolist()

    # 3) Obter serviço do Sheets
    _, sheets_service = get_services()

    # 4) Limpar planilha
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id, range="A:ZZ"
    ).execute()

    # 5) Escrever dados
    body = {"values": values}
    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="A1",
        valueInputOption="RAW",
        body=body,
    ).execute()