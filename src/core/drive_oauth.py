from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

# Escopo: criar/atualizar arquivos que o app criar/abrir
SCOPES = ["https://www.googleapis.com/auth/drive.file"]

BASE_DIR = Path(__file__).resolve().parents[2]
CREDS_PATH = BASE_DIR / "credentials" / "oauth_client.json"
TOKEN_PATH = BASE_DIR / "credentials" / "oauth_token.json"


def get_drive_service_oauth():
    """
    Cria um cliente do Drive autenticado como USUÁRIO (OAuth Desktop).
    Na primeira execução abre o navegador; depois reutiliza o token salvo.
    """
    creds = None

    # 1) Se já existe token salvo, tenta carregar
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    # 2) Se não tem credencial válida, renova ou faz login
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())  # renova sem abrir navegador
        else:
            flow = InstalledAppFlow.from_client_secrets_file(str(CREDS_PATH), SCOPES)
            creds = flow.run_local_server(port=0)  # abre navegador 1x

        # 3) Salva token para não precisar logar de novo
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    return build("drive", "v3", credentials=creds)
