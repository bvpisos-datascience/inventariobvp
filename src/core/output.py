from __future__ import annotations

from pathlib import Path
from typing import List, Any, Optional
from datetime import datetime
import pandas as pd
import numpy as np
from googleapiclient.errors import HttpError
from .google_client import get_sheets_service
from .config_ingestion import (
    INGESTION_GOOGLE_SHEET_ID,
    INGESTION_GOOGLE_SHEET_TAB,
)

# =========================================================
# CSV LOCAL
# =========================================================

def write_csv_local(df: pd.DataFrame, base_dir: Path) -> Path:
    """
    Salva o DataFrame final em CSV localmente.
    Retorna o caminho do arquivo salvo.
    """
    if df.empty:
        raise ValueError("[OUTPUT] DataFrame vazio — abortando escrita do CSV")

    output_dir = base_dir / "_outputs"
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "base_inventario_consolidada.csv"

    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print(f"[OUTPUT] CSV salvo localmente: {csv_path}")

    return csv_path


# =========================================================
# GOOGLE DRIVE (placeholder / gancho)
# =========================================================

def upload_csv_to_drive(csv_path: Path) -> Optional[str]:
    """
    Gancho para upload no Google Drive.
    Pode ser implementado depois.
    
    Returns:
        None (por enquanto) ou file_id quando implementado
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"[OUTPUT] Arquivo CSV não encontrado: {csv_path}"
        )

    # Implementação futura
    print(f"[OUTPUT] Upload para Google Drive pendente: {csv_path}")
    return None


# =========================================================
# GOOGLE SHEETS
# =========================================================


def _df_to_values(df: pd.DataFrame) -> List[List[Any]]:
    """
    Converte DataFrame para formato aceito pelo Google Sheets API,
    preservando tipos numéricos e tratando corretamente valores nulos.
    
    CORREÇÃO: Garante que NaN/Inf (float nativo ou numpy) virem None.
    """
    df_clean = df.copy()
    
    # 1. Converte TODAS as colunas datetime/timestamp para string
    for col in df_clean.columns:
        if pd.api.types.is_datetime64_any_dtype(df_clean[col]):
            mask = df_clean[col].notna()
            df_clean.loc[mask, col] = df_clean.loc[mask, col].dt.strftime('%Y-%m-%d %H:%M:%S')
            df_clean.loc[~mask, col] = None
    
    # 2. Substitui NaN por None de forma vetorizada (para objetos/strings)
    # Nota: Isso nem sempre pega floats numpy puros, por isso tratamos no loop abaixo
    df_clean = df_clean.where(pd.notna(df_clean), None)
    
    # 3. Converte para lista - Header primeiro
    values = [df_clean.columns.tolist()]
    
    # 4. Converte valores para tipos nativos do Python
    for row in df_clean.values.tolist():
        clean_row = []
        for val in row:
            # None já está correto (vira null no JSON)
            if val is None:
                clean_row.append(None)
            
            # Bool numpy ou nativo
            elif isinstance(val, (bool, np.bool_)):
                clean_row.append(bool(val))
            
            # Inteiros numpy ou nativo
            elif isinstance(val, (int, np.integer)):
                clean_row.append(int(val))
            
            # Floats (Numpy OU Nativo do Python)
            # AQUI ESTAVA O ERRO: native float('nan') passava batido
            elif isinstance(val, (float, np.floating)):
                # Se for Infinito ou NaN, deve virar None
                if np.isnan(val) or np.isinf(val):
                    clean_row.append(None)
                else:
                    clean_row.append(float(val))
            
            # Timestamp restante (segurança extra)
            elif isinstance(val, pd.Timestamp):
                clean_row.append(val.strftime('%Y-%m-%d %H:%M:%S'))
            
            # Strings e outros objetos
            else:
                # Convertemos para string para evitar erros de serialização de objetos estranhos
                clean_row.append(str(val))
                
        values.append(clean_row)
    
    return values


def update_google_sheet(
    df: pd.DataFrame,
    spreadsheet_id: str = INGESTION_GOOGLE_SHEET_ID,
    sheet_name: str = INGESTION_GOOGLE_SHEET_TAB,
) -> None:
    """
    ATUALIZA uma aba existente de uma Google Sheet existente.
    
    Comportamento:
    - Limpa TODO o conteúdo da aba
    - Reescreve do zero com novos dados
    - Não cria planilha (deve existir)
    - Não cria aba (deve existir)
    
    Args:
        df: DataFrame com os dados a serem escritos
        spreadsheet_id: ID da planilha do Google Sheets
        sheet_name: Nome da aba/sheet a ser atualizada
        
    Raises:
        ValueError: Se DataFrame estiver vazio ou sheet_name inválido
        RuntimeError: Se houver erro na comunicação com Google Sheets API
    """
    # ---- Validações
    if df.empty:
        raise ValueError("[OUTPUT] DataFrame vazio — abortando update da Sheet")
    
    if not sheet_name or not sheet_name.strip():
        raise ValueError("[OUTPUT] Nome da aba não pode ser vazio")
    
    if not spreadsheet_id or not spreadsheet_id.strip():
        raise ValueError("[OUTPUT] ID da planilha não pode ser vazio")

    # ---- Cópia defensiva + timestamp de execução
    df = df.copy()
    df["_last_pipeline_update"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("[OUTPUT] Atualizando Google Sheet")
    print(f"[OUTPUT] Linhas: {len(df)}, Colunas: {len(df.columns)}")
    print(f"[OUTPUT] Planilha: {spreadsheet_id}")
    print(f"[OUTPUT] Aba: '{sheet_name}'")

    # ---- Prepara dados
    service = get_sheets_service()
    values = _df_to_values(df)
    range_all = f"{sheet_name}"

    try:
        # 1) Limpa a aba inteira
        print("[OUTPUT] Limpando aba...")
        service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=range_all,
            body={},
        ).execute()

        # 2) Reescreve tudo (USER_ENTERED permite inferência de tipos)
        print("[OUTPUT] Escrevendo dados...")
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()

        print(
            "[OUTPUT] ✓ Google Sheet atualizada com sucesso | "
            f"sheet_id={spreadsheet_id} | aba='{sheet_name}'"
        )

    except HttpError as e:
        error_msg = (
            f"[OUTPUT] Erro ao atualizar Google Sheet | "
            f"sheet_id={spreadsheet_id} | aba='{sheet_name}' | "
            f"Detalhes: {e}"
        )
        raise RuntimeError(error_msg) from e
    except Exception as e:
        error_msg = (
            f"[OUTPUT] Erro inesperado ao atualizar Google Sheet | "
            f"sheet_id={spreadsheet_id} | aba='{sheet_name}' | "
            f"Erro: {type(e).__name__}: {e}"
        )
        raise RuntimeError(error_msg) from e