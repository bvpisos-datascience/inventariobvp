# utils.py
import os
import io
import json
from pathlib import Path

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

# -------------------------------------------------------
# 1) Montagem das credenciais de forma segura
#    - Local: lê do .env / arquivo JSON
#    - Streamlit Cloud: usa st.secrets["GOOGLE_CREDENTIALS"]
# -------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

def _build_credentials():
    """Retorna um objeto Credentials para Drive + Sheets."""
    # Ambiente Streamlit Cloud
    if os.getenv("STREAMLIT_RUNTIME"):
        import streamlit as st

        # JSON bruto vindo dos secrets (GOOGLE_CREDENTIALS)
        creds_json = st.secrets["GOOGLE_CREDENTIALS"]
        info = json.loads(creds_json)

        creds = service_account.Credentials.from_service_account_info(
            info, scopes=SCOPES
        )
        return creds

    # Ambiente local: usa GOOGLE_APPLICATION_CREDENTIALS (arquivo JSON)
    from dotenv import load_dotenv

    load_dotenv()
    sa_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    if not sa_path:
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS não definido. "
            "Configure no .env ou nos secrets do Streamlit."
        )

    sa_path = Path(sa_path)
    if not sa_path.exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado: {sa_path}")

    creds = service_account.Credentials.from_service_account_file(
        str(sa_path), scopes=SCOPES
    )
    return creds


# Cria cliente de Drive e Sheets reutilizáveis
creds = _build_credentials()
drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)


# -------------------------------------------------------
# 2) Funções utilitárias
# -------------------------------------------------------

def list_gsheets_in_folder(folder_id: str):
    """
    Lista arquivos XLSX em uma pasta do Drive.
    Retorna lista de dicts: [{id: ..., name: ...}, ...]
    """
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
    Faz download do XLSX do Drive e lê como DataFrame.
    (Apesar do nome, os arquivos são XLSX, não Google Sheets nativos.)
    """
    data = drive_service.files().get_media(fileId=file_id).execute()
    bio = io.BytesIO(data)
    df = pd.read_excel(bio)
    return df


def write_output(df: pd.DataFrame, destino: str = "sheets"):
    """
    Grava o DataFrame:
    - sempre salva CSV local em _outputs/base_inventario_12meses.csv
    - se destino == 'sheets', escreve na planilha definida em SHEET_OUTPUT_ID
    """

    # 1) Salvar histórico local (quando estiver rodando localmente)
    hist_path = os.getenv("HIST_SOURCE", "_outputs/base_inventario_12meses.csv")
    Path(hist_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(hist_path, index=False, encoding="utf-8-sig")

    if destino != "sheets":
        return

    sheet_id = os.getenv("SHEET_OUTPUT_ID")
    if not sheet_id:
        raise RuntimeError(
            "SHEET_OUTPUT_ID não definido. Configure no .env ou nos secrets."
        )

    # Converte DataFrame para matriz de valores
    df_out = df.copy()
    df_out = df_out.astype(object).where(pd.notna(df_out), "")

    values = [list(df_out.columns)] + df_out.values.tolist()

    # Limpa a planilha (aba padrão)
    sheets_service.spreadsheets().values().clear(
        spreadsheetId=sheet_id, range="A:ZZ"
    ).execute()

    # Escreve novos dados
    body = {"values": values}

    sheets_service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range="A1",
        valueInputOption="RAW",
        body=body,
    ).execute()
