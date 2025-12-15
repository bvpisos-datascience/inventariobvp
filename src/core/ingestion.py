"""
Módulo de Ingestão de Dados

Responsável por:
- Listar arquivos no Google Drive (via google_client)
- Identificar tipo (Sheet ou Excel)
- Ler conteúdo e transformar em DataFrame
- Retornar lista de DataFrames ou um consolidado final

Este módulo NÃO faz transformações pesadas (isso é da Fase 4).
"""

import pandas as pd
import io
from xlrd.biffh import XLRDError  

from .google_client import (
    list_files_in_input_folder,
    get_drive_service,
    get_gspread_client
)

def detecta_tipo_arquivo(file_name: str, mime_Type: str) -> str:
    '''
    Retorna tipo de arquivo com base na extensão ou mimeType.
    '''
    name = file_name.lower()

    if mime_Type ==  "application/vnd.google-apps.spreadsheet":
        return 'gsheet'
    
    if name.endswith('.xlsx') or mime_Type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return "xlsx"
    
    if name.endswith('.xls') or mime_Type == "application/vnd.ms-excel":
        return "xls"
    
    return 'unknown'

def read_gsheet(file_id: str) -> pd.DataFrame:
    '''
    Lê um arquivo googel sheet e retorna um data frame
    '''
    client = get_gspread_client() 
    sheet = client.open_by_key(file_id).sheet1 

    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    return df

def read_excel_from_drive(file_id: str, file_name: str, mime_Type: str) -> pd.DataFrame:
    '''
    Lê arquivos formatos excel do google drive e retorna um data frame
    '''
    service = get_drive_service()

    request = service.files().get_media(fileId=file_id)
    file_bytes =  request.execute()

    in_memory = io.BytesIO(file_bytes) 

    name = file_name.lower() 

    if name.endswith('.xlsx') or mime_Type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        engine='openpyxl'
    elif name.endswith('.xls') or mime_Type == "application/vnd.ms-excel":
        engine = 'xlrd'
    else:
        engine = None 

    try:
        if engine is None:
            df = pd.read_excel(in_memory)
        else:
            df = pd.read_excel(in_memory, engine=engine)
    
        return df 
    
    except XLRDError as e:
        print(f"[ERRO XLS/XLRD] Não foi possível ler o arquivo {file_name} como XLS {e}")
        in_memory.seek(0)
        try:
            tables = pd.read_html(in_memory)
            if tables:
                print(f"[INFO] '{file_name}' lido como HTML/XML (planilha em formato XML).")
                return tables[0]
        except Exception as e2:
            print(f"[ERRO Fallback HTML/XML] Também falhou para '{file_name}': {e2}")

        # Último recurso: devolve DataFrame vazio
        return pd.DataFrame()

    except Exception as e:
        print(f"[ERRO GENÉRICO] Problema ao ler '{file_name}': {e}")
        return pd.DataFrame()

def load_all_files_as_dataframes(max_files=50):
    '''
    Lê todos os arquivos da pasta de entrada e retorna uma lista de data frames. 
    '''

    files = list_files_in_input_folder(max_files=max_files) 
    dataframes = [] 

    for f in files:
        file_id = f['id']
        file_name = f['name']
        mime_Type = f['mimeType']

        file_type = detecta_tipo_arquivo(file_name, mime_Type)

        if file_type == 'gsheet':
            df = read_gsheet(file_id) 

        elif file_type in ("xlsx", "xls"):
            df = read_excel_from_drive(file_id, file_name, mime_Type)

        else:
            print(f" Aviso: ignorando arquivo desconhecido: {file_name}")
            continue

        dataframes.append(df)

    return dataframes

def consolidate_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    '''
    Concatena vários data frames em um único data frame.
    '''
    if not dataframes:
        return pd.DataFrame()
    
    df_final = pd.concat(dataframes, ignore_index=True)
    return df_final 

if __name__ == "__main__":
    dfs = load_all_files_as_dataframes() 

    print(f"Foram lidos {len(dfs)} arquivos.")
    df_final = consolidate_dataframes(dfs) 

    print(f"DataFrame final: {df_final.shape[0]} linhas e {df_final.shape[1]} colunas.")
    print(df_final.head())

