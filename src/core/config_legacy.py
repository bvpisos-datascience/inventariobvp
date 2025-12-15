import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parents[2]
load_dotenv(BASE_DIR / ".env")

LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID = os.getenv("LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID")
LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID = os.getenv("LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID")

if not LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID:
    raise RuntimeError("LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID não definido no .env")

if not LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID:
    raise RuntimeError("LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID não definido no .env")
