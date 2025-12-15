from pathlib import Path
from dotenv import load_dotenv
import os

# Carregar variáveis .env
BASE_DIR = Path(__file__).resolve().parents[2] # caminho até config.py -  /Projeto_Inventário/indicadores_inventario/src/core/config.py"
ENV_PATH = BASE_DIR /  ".env"
load_dotenv(ENV_PATH)

# Exemplo de variáveis de ambiente
GOOGLE_SERVICE_ACCOUNT_FILE = BASE_DIR / "credentials" / os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "indicadores-inventario-bv.json")
GOOGLE_DRIVE_INPUT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_INPUT_FOLDER_ID", "1LlQo0S8EqxKz4mIkjvSxGtaL9ZI9Ie0n")
GOOGLE_DRIVE_OUTPUT_FOLDER_ID = os.getenv("GOOGLE_DRIVE_OUTPUT_FOLDER_ID", "1cx0TxyinFTEp5ENbisdoYF6K19W8aSOK")