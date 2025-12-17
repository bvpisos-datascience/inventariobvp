from __future__ import annotations

from pathlib import Path
import pandas as pd

from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

from .google_client import get_drive_service
from .config_ingestion import INGESTION_GOOGLE_DRIVE_OUTPUT_FOLDER_ID
from .drive_oauth import get_drive_service_oauth


def write_csv_local(
    df: pd.DataFrame,
    base_dir: Path,
    filename: str = "base_inventario_consolidada.csv",
) -> Path:
    out_dir = base_dir / "_outputs"
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / filename
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"[OUTPUT] CSV salvo localmente: {csv_path}")

    return csv_path


def _find_file_in_folder(service, folder_id: str, filename: str) -> str | None:
    safe_name = filename.replace("'", "\\'")
    q = f"'{folder_id}' in parents and name = '{safe_name}' and trashed = false"

    res = service.files().list(
        q=q,
        pageSize=1,
        fields="files(id, name)",
    ).execute()

    files = res.get("files", [])
    return files[0]["id"] if files else None


def _update_only(service, csv_path: Path, folder_id: str) -> str:
    """
    ATUALIZA arquivo existente.
    Se não encontrar o arquivo -> ERRO.
    """
    existing_id = _find_file_in_folder(service, folder_id, csv_path.name)
    if not existing_id:
        raise RuntimeError(
            f"[OUTPUT] Arquivo '{csv_path.name}' não encontrado na pasta {folder_id}. "
            "Criação de arquivos está DESABILITADA."
        )

    media = MediaFileUpload(
        str(csv_path),
        mimetype="text/csv",
        resumable=False,
    )

    updated = service.files().update(
        fileId=existing_id,
        media_body=media,
        fields="id",
    ).execute()

    print(f"[OUTPUT] CSV atualizado no Drive: {csv_path.name} (id={updated['id']})")
    return updated["id"]


def upload_csv_to_drive(
    csv_path: Path,
    folder_id: str = INGESTION_GOOGLE_DRIVE_OUTPUT_FOLDER_ID,
    prefer_oauth: bool = False,
) -> str:
    """
    Upload UPDATE-ONLY:
    - tenta Service Account
    - se erro de cota -> fallback OAuth
    - NUNCA cria arquivo
    """
    # 1) OAuth direto (se forçado)
    if prefer_oauth:
        service = get_drive_service_oauth()
        return _update_only(service, csv_path, folder_id)

    # 2) Tenta Service Account
    try:
        service_sa = get_drive_service()
        return _update_only(service_sa, csv_path, folder_id)

    except HttpError as e:
        msg = ""
        try:
            msg = (
                e.content.decode("utf-8", errors="ignore")
                if hasattr(e, "content") and e.content
                else str(e)
            )
        except Exception:
            msg = str(e)

        # 3) Fallback específico: cota de Service Account
        if "storageQuotaExceeded" in msg or "Service Accounts do not have storage quota" in msg:
            print(
                "[OUTPUT] Service Account sem cota para gravar no Drive. "
                "Usando OAuth (usuário) para UPDATE..."
            )
            service_oauth = get_drive_service_oauth()
            return _update_only(service_oauth, csv_path, folder_id)

        raise
