from __future__ import annotations

from typing import Any

import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build

from .config_ingestion import GOOGLE_SERVICE_ACCOUNT_FILE

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]


def get_credentials():
    return service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=SCOPES,
    )


def get_drive_service():
    return build("drive", "v3", credentials=get_credentials())

def get_sheets_service():
    return build("sheets", "v4", credentials=get_credentials())


def get_gspread_client():
    return gspread.authorize(get_credentials())


def list_files_in_folder(folder_id: str, max_files: int = 450) -> list[dict[str, Any]]:
    """
    Lista arquivos dentro de uma pasta do Google Drive (com paginação).
    Retorna: id, name, mimeType.
    """
    if not folder_id or not str(folder_id).strip():
        raise ValueError("folder_id inválido")

    service = get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"

    files: list[dict[str, Any]] = []
    page_token = None

    while True:
        res = (
            service.files()
            .list(
                q=query,
                pageSize=min(1000, max_files),
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
            )
            .execute()
        )

        files.extend(res.get("files", []))

        if len(files) >= max_files:
            return files[:max_files]

        page_token = res.get("nextPageToken")
        if not page_token:
            break

    return files
