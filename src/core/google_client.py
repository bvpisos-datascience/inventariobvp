from google.oauth2 import service_account 
from googleapiclient.discovery import build 
import gspread 

from  .config import GOOGLE_SERVICE_ACCOUNT_FILE 

# Escopos de acesso
SCOPES = [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/spreadsheets'
]

def get_credentials():
    '''
    Carrega as credenciais da service account a partir do arquivo JSON
    definido em GOOGLE_SERVICE_ACCOUNT_FILE.
    '''
    credentials = service_account.Credentials.from_service_account_file(
        GOOGLE_SERVICE_ACCOUNT_FILE,
        scopes=SCOPES           
    )
    return credentials


def get_drive_service():
    '''
    Retorna um cliente do Google Drive API autitenicado.
    '''
    creds = get_credentials()
    service = build("drive", "v3", credentials=creds)
    return service

def get_gspread_client():
    '''
    Retorna um cliente gspread autenticado com service account.
    '''
    creds = get_credentials()
    client = gspread.authorize(creds)
    return client 


from .config import GOOGLE_DRIVE_INPUT_FOLDER_ID 

def list_files_in_input_folder(max_files=10):
    '''
    Lista alguns arquivos da pasta de entrada no Google Drive
    para testar se a autenticação está funcionando
    '''
    service = get_drive_service()

    query = f"'{GOOGLE_DRIVE_INPUT_FOLDER_ID}' in parents and trashed = false"

    results = service.files().list(
        q=query,
        pageSize=max_files,
        fields="files(id, name, mimeType)"
    ).execute()

    files = results.get("files", [])

    return files

if __name__ == "__main__":
    files = list_files_in_input_folder()
    print("Arquivos encontrados na pasta de entrada:")
    for f in files:
        print(f"- {f['name']} ({f['id']})")