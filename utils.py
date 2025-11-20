import os
import io
from typing import List, Dict, Optional

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Carrega variáveis do .env
load_dotenv(override=True)

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not SERVICE_ACCOUNT_FILE:
    raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS não definido no .env")

creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES,
)

drive_service = build("drive", "v3", credentials=creds)
sheets_service = build("sheets", "v4", credentials=creds)


# ---------------------------------------------------------------------
# HELPER: detectar linha de cabeçalho dentro da aba
# ---------------------------------------------------------------------
def _detect_header_row(df_no_header: pd.DataFrame) -> Optional[int]:
    """
    Tenta identificar em qual linha está o cabeçalho da tabela de inventário.
    Critério simples: linha que contenha 'qtd', 'erp' e 'wms' em algum lugar.
    """
    expected_tokens = ["qtd", "erp", "wms"]

    for idx, row in df_no_header.iterrows():
        text = " ".join(str(x).lower() for x in row.tolist() if pd.notna(x))
        if all(tok in text for tok in expected_tokens):
            return idx

    return None


# ---------------------------------------------------------------------
# LISTAR ARQUIVOS NA PASTA DE ENTRADA
# ---------------------------------------------------------------------
def list_gsheets_in_folder(folder_id: str) -> List[Dict]:
    """
    Lista todos os arquivos dentro de uma pasta do Drive (não só Google Sheets),
    retornando uma lista de dicts com pelo menos: id, name.
    """
    q = f"'{folder_id}' in parents and trashed = false"

    files: List[Dict] = []
    page_token = None

    while True:
        resp = drive_service.files().list(
            q=q,
            spaces="drive",
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
        ).execute()

        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files


# ---------------------------------------------------------------------
# LER ARQUIVO DO DRIVE EM DATAFRAME (detectando cabeçalho real)
# ---------------------------------------------------------------------
def read_gsheet_to_df(file_id: str) -> pd.DataFrame:
    """
    Dado um file_id do Drive, baixa o arquivo (Google Sheet ou XLSX),
    percorre todas as abas e tenta identificar a linha de cabeçalho que
    contém Qtd. ERP / Qtd. WMS / etc.

    Retorna um DataFrame pandas com os dados da tabela de inventário.
    """
    meta = drive_service.files().get(
        fileId=file_id,
        fields="mimeType, name",
        supportsAllDrives=True,
    ).execute()

    mime_type = meta["mimeType"]
    name = meta["name"]

    # 1) Download/export para binário XLSX
    if mime_type == "application/vnd.google-apps.spreadsheet":
        request = drive_service.files().export_media(
            fileId=file_id,
            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        request = drive_service.files().get_media(fileId=file_id)

    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

    fh.seek(0)

    # 2) Abre como Excel e procura a aba/linha com cabeçalho de inventário
    try:
        xls = pd.ExcelFile(fh)
    except Exception as e:
        raise RuntimeError(f"Falha ao abrir arquivo '{name}' como Excel: {e}")

    for sheet_name in xls.sheet_names:
        tmp = xls.parse(sheet_name, header=None)  # nenhuma linha é header ainda
        header_idx = _detect_header_row(tmp)
        if header_idx is None:
            continue

        # Essa linha vira cabeçalho
        header = tmp.iloc[header_idx].astype(str).tolist()
        data = tmp.iloc[header_idx + 1 :].copy()
        data.columns = header
        data = data.dropna(how="all")  # remove linhas completamente vazias

        return data

    # Se nenhuma aba bateu com o padrão esperado
    raise RuntimeError(
        f"Nenhuma aba de inventário com cabeçalho contendo 'Qtd', 'ERP' e 'WMS' "
        f"encontrada no arquivo '{name}' ({file_id})."
    )


# ---------------------------------------------------------------------
# SUPORTE PARA ESCRITA DA BASE CONSOLIDADA EM GOOGLE SHEETS
# ---------------------------------------------------------------------
def _find_or_create_output_sheet(folder_id: str, name: str) -> str:
    """
    Procura uma planilha Google com um determinado nome dentro de uma pasta.
    Se não encontrar, cria uma nova e retorna o ID.
    """
    q = (
        f"'{folder_id}' in parents and "
        "mimeType='application/vnd.google-apps.spreadsheet' "
        f"and name='{name}' and trashed = false"
    )

    resp = drive_service.files().list(
        q=q,
        fields="files(id, name)",
        pageSize=1,
        includeItemsFromAllDrives=True,
        supportsAllDrives=True,
    ).execute()

    files = resp.get("files", [])
    if files:
        return files[0]["id"]

    # Não existe -> cria
    file_metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.spreadsheet",
        "parents": [folder_id],
    }

    sheet = drive_service.files().create(
        body=file_metadata,
        fields="id",
        supportsAllDrives=True,
    ).execute()

    return sheet["id"]


import datetime as dt  # garante que exista esse import no topo do arquivo

def _df_to_sheet_values(df: pd.DataFrame):
    """
    Converte um DataFrame em lista de listas para usar no Sheets API.
    - Substitui NaN por string vazia
    - Converte Timestamp / datetime / date para string ISO (YYYY-MM-DD HH:MM:SS)
    """
    values = [list(df.columns)]

    for row in df.itertuples(index=False):
        linha = []
        for v in row:
            if pd.isna(v):
                linha.append("")
            elif isinstance(v, (pd.Timestamp, dt.datetime, dt.date)):
                # converte para string legível pelo Sheets
                linha.append(str(v))
            else:
                linha.append(v)
        values.append(linha)

    return values



def write_output(df: pd.DataFrame, destino: str):
    """
    Escreve o DataFrame consolidado no destino configurado.

    Se DESTINO == 'sheets':

        - Se SHEET_OUTPUT_ID estiver definido:
            escreve diretamente nessa planilha (ID fixo).

        - Caso contrário:
            cria/atualiza a planilha 'base_inventario_12meses'
            dentro da pasta DRIVE_FOLDER_OUTPUT.

        - Em ambos os casos, pode salvar um CSV local em HIST_SOURCE (se definido).

    Se DESTINO != 'sheets':
        - Salva apenas CSV local em HIST_SOURCE (fallback).
    """
    hist_source = os.getenv("HIST_SOURCE")

    if destino == "sheets":
        # 1º: tenta usar SHEET_OUTPUT_ID, se existir
        sheet_id = os.getenv("SHEET_OUTPUT_ID")

        if not sheet_id:
            # Fallback: usar pasta e criar/achar a planilha
            output_folder_id = os.getenv("DRIVE_FOLDER_OUTPUT")
            if not output_folder_id:
                raise ValueError(
                    "Nem SHEET_OUTPUT_ID nem DRIVE_FOLDER_OUTPUT definidos no .env. "
                    "Defina pelo menos um."
                )

            sheet_id = _find_or_create_output_sheet(
                output_folder_id,
                "base_inventario_12meses",
            )

        # Converte DF para matriz de valores
        values = _df_to_sheet_values(df)

        # Limpa a planilha antes de escrever
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=sheet_id,
            range="A:ZZ",
        ).execute()

        # Escreve a partir de A1
        sheets_service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range="A1",
            valueInputOption="RAW",
            body={"values": values},
        ).execute()

        # (Opcional) salvar CSV local para histórico
        if hist_source:
            os.makedirs(os.path.dirname(hist_source), exist_ok=True)
            df.to_csv(hist_source, index=False)

    else:
        # Somente CSV local
        if not hist_source:
            hist_source = "_outputs/base_inventario_12meses.csv"

        os.makedirs(os.path.dirname(hist_source), exist_ok=True)
        df.to_csv(hist_source, index=False)

