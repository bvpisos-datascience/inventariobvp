from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv


# Raiz do projeto (ajuste o parents[] se sua estrutura for diferente)
BASE_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = BASE_DIR / ".env"

# Carrega o .env (não falha se não existir; as validações abaixo é que garantem)
load_dotenv(ENV_PATH)


def _require_env(name: str) -> str:
    val = os.getenv(name)
    if not val or not str(val).strip():
        raise RuntimeError(f"{name} não definido no .env (lido em: {ENV_PATH})")
    return str(val).strip().strip('"').strip("'")


# --- Credencial (service account file) ---
# Você definiu GOOGLE_APPLICATION_CREDENTIALS no .env
GOOGLE_APPLICATION_CREDENTIALS = _require_env("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_SERVICE_ACCOUNT_FILE = GOOGLE_APPLICATION_CREDENTIALS  # alias compatível com o resto do código

cred_path = Path(GOOGLE_SERVICE_ACCOUNT_FILE)
if not cred_path.exists():
    raise RuntimeError(f"Arquivo de credencial não encontrado: {cred_path}")
if cred_path.suffix.lower() != ".json":
    raise RuntimeError(f"Credencial precisa ser .json. Recebido: {cred_path}")


# --- Pastas Drive usadas pela pipeline ---
INGESTION_GOOGLE_DRIVE_INPUT_FOLDER_ID = _require_env("INGESTION_GOOGLE_DRIVE_INPUT_FOLDER_ID")
INGESTION_GOOGLE_DRIVE_OUTPUT_FOLDER_ID = _require_env("INGESTION_GOOGLE_DRIVE_OUTPUT_FOLDER_ID")

# --- Google Sheets (OUTPUT) ---
INGESTION_GOOGLE_SHEET_ID = _require_env("INGESTION_GOOGLE_SHEET_ID")
INGESTION_GOOGLE_SHEET_TAB = _require_env("INGESTION_GOOGLE_SHEET_TAB")
