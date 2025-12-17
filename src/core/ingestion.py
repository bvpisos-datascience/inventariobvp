# -*- coding: utf-8 -*-
"""
Módulo de Ingestão de Dados (Fase 3)

Responsável por:
- Listar arquivos válidos no Google Drive
- Ler Google Sheets
- Ler arquivos Excel modernos (.xlsx)
- Retornar DataFrames confiáveis

Premissa:
- Arquivos legados (.xls) já foram tratados pelo conversor legacy
"""

from __future__ import annotations  # <- CRÍTICO: evita quebrar import por type hints

import io
import time
import random

import pandas as pd
from googleapiclient.http import MediaIoBaseDownload

from .config_ingestion import INGESTION_GOOGLE_DRIVE_INPUT_FOLDER_ID
from .google_client import (
    list_files_in_folder,
    get_drive_service,
    get_gspread_client,
)


def detecta_tipo_arquivo(file_name: str, mime_type: str) -> str:
    name = (file_name or "").lower()
    mime_type = (mime_type or "").lower()

    if mime_type == "application/vnd.google-apps.spreadsheet":
        return "gsheet"

    if name.endswith(".xlsx"):
        return "xlsx"

    return "unknown"


def read_gsheet(file_id: str) -> pd.DataFrame:
    """
    Lê um Google Sheet e retorna um DataFrame.
    Usa a primeira aba (sheet1).
    """
    client = get_gspread_client()
    ws = client.open_by_key(file_id).sheet1

    data = ws.get_all_records()
    if data:
        return pd.DataFrame(data)

    # fallback: valores brutos
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    return pd.DataFrame(rows, columns=header)


def _find_header_row(preview: pd.DataFrame):
    """
    Detecta a linha de cabeçalho num preview sem header.
    Critério: existir 'item' e 'descricao/descrição' na mesma linha.
    """
    for i in range(len(preview)):
        row = preview.iloc[i].astype(str).str.strip().str.lower()

        # normaliza alguns acentos comuns só para comparação
        row = (
            row.str.replace("ç", "c", regex=False)
               .str.replace("ã", "a", regex=False)
               .str.replace("á", "a", regex=False)
               .str.replace("à", "a", regex=False)
               .str.replace("â", "a", regex=False)
               .str.replace("é", "e", regex=False)
               .str.replace("ê", "e", regex=False)
               .str.replace("í", "i", regex=False)
               .str.replace("ó", "o", regex=False)
               .str.replace("ô", "o", regex=False)
               .str.replace("ú", "u", regex=False)
        )

        vals = set(row.values)
        if ("item" in vals) and ("descricao" in vals):
            return i

    return None


def read_excel_from_drive(file_id: str) -> pd.DataFrame:
    """
    Faz download do XLSX via Drive API e lê em DataFrame.
    Detecta automaticamente a linha do cabeçalho.
    Retentativas com backoff exponencial.
    """
    service = get_drive_service()

    last_err = None
    for attempt in range(1, 6):
        try:
            request = service.files().get_media(fileId=file_id)

            bio = io.BytesIO()
            downloader = MediaIoBaseDownload(bio, request, chunksize=1024 * 1024)

            done = False
            while not done:
                _status, done = downloader.next_chunk(num_retries=5)

            bio.seek(0)

            preview = pd.read_excel(bio, engine="openpyxl", header=None, nrows=30)
            header_row = _find_header_row(preview)
            if header_row is None:
                raise ValueError("Não encontrei a linha de cabeçalho (ex: 'Item' e 'Descrição').")

            bio.seek(0)
            df = pd.read_excel(bio, engine="openpyxl", header=header_row)

            df = df.dropna(axis=1, how="all")
            df = df.dropna(axis=0, how="all")

            return df

        except Exception as e:
            last_err = e
            wait = min(60, 2 ** attempt) + random.uniform(0, 1.5)
            print(
                f"[INGESTION] Falha ao baixar/ler (tentativa {attempt}/5). "
                f"{type(e).__name__}: {e}. Retentando em {wait:.1f}s..."
            )
            time.sleep(wait)

    raise RuntimeError(f"[INGESTION] Falha após 5 tentativas. Último erro: {type(last_err).__name__}: {last_err}")


def load_all_files_as_dataframes(max_files: int = 450):
    """
    Lê todos os arquivos da pasta de entrada e retorna lista de DataFrames.
    """
    files = list_files_in_folder(INGESTION_GOOGLE_DRIVE_INPUT_FOLDER_ID, max_files=max_files)

    dataframes = []
    for f in files:
        file_id = f.get("id")
        file_name = f.get("name", "")
        mime_type = f.get("mimeType", "")

        if not file_id:
            print(f"[INGESTION] Aviso: arquivo sem id, ignorando: {file_name}")
            continue

        kind = detecta_tipo_arquivo(file_name, mime_type)

        if kind == "gsheet":
            df = read_gsheet(file_id)
        elif kind == "xlsx":
            df = read_excel_from_drive(file_id)
        else:
            print(f"[INGESTION] Aviso: ignorando arquivo desconhecido: {file_name}")
            continue

        if df is None or df.empty:
            print(f"[INGESTION] Aviso: DF vazio, ignorando: {file_name}")
            continue

        dataframes.append(df)

    return dataframes


def consolidate_dataframes(dataframes):
    """
    Concatena vários DataFrames em um único DataFrame.
    """
    if not dataframes:
        return pd.DataFrame()
    return pd.concat(dataframes, ignore_index=True)


if __name__ == "__main__":
    dfs = load_all_files_as_dataframes(max_files=50)
    print(f"Foram lidos {len(dfs)} arquivos válidos.")
    df_final = consolidate_dataframes(dfs)
    print(f"DataFrame final: {df_final.shape[0]} linhas e {df_final.shape[1]} colunas.")
    print(df_final.head())
