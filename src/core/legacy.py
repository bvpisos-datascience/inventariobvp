"""
Conversor one-shot de arquivos .xls legados no Google Drive para .xlsx reais.

Fluxo:
1) Lista arquivos na pasta INPUT do Drive
2) Para cada arquivo com extensão .xls:
   - baixa para pasta local (tmp/_xls_legacy)
   - converte para .xlsx (preferencialmente via Microsoft Excel COM)
   - envia .xlsx para pasta OUTPUT do Drive
   - (opcional) pode renomear/mover o original, mas aqui vamos só subir o .xlsx

Como executar (na raiz do projeto):
  python -m src.core.convert_xls_legacy
"""

from __future__ import annotations
from pathlib import Path

from .config_legacy import (
    LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID,
    LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID,
)


from .google_client import get_drive_service

# -----------------------------
# Pastas locais temporárias
# -----------------------------

BASE_DIR = Path(__file__).resolve().parents[2]
TMP_DIR = BASE_DIR / "data" / "tmp_xls_legacy"
RAW_DIR = TMP_DIR / "raw_xls"
OUT_DIR = TMP_DIR / "converted_xlsx"

# -----------------------------
# 1) Helpers do Drive
# -----------------------------

def list_drive_files(folder_id: str, max_files: int = 500) -> list[dict]:
    """
    Lista arquivos na pasta do Drive (id, name, mimeType).
    """

    service = get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"

    results = service.files().list(
        q=query,
        pageSize=max_files,
        fields="files(id, name, mimeType)"
    ).execute()

    return results.get("files", [])

def download_drive_file(file_id: str, dest_path: Path) -> None:
    """
    Baixa um arquivo do Drive (binário) e salva em dest_path.
    """

    service = get_drive_service()
    request = service.files(). get_media(fileId=file_id)
    content = request.execute()

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(content)

def upload_xlsx_to_drive(local_path: Path, drive_folder_id: str) -> str:
    """
    Faz upload de um .xlsx local para uma pasta do Drive.
    Retorna o ID do arquivo criado no Drive.
    """
    from .drive_oauth import get_drive_service_oauth
    from googleapiclient.http import MediaFileUpload

    service = get_drive_service_oauth()

    file_metadata = {
        "name": local_path.name, 
        "parents": [drive_folder_id]
    }

    media = MediaFileUpload(
        str(local_path),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=True
    )

    created = service.files().create(
        body=file_metadata,
        media_body=media,
        fields="id"
    ).execute()

    return created["id"]

# -----------------------------
# 2) Conversão XLS -> XLSX

def convert_xls_to_xlsx_excel_com(xls_path: Path, xlsx_path: Path) -> None:
    """
    Converte .xls para .xlsx usando Microsoft Excel via COM.
    Requer Excel instalado + pywin32.
    """
    import win32com.client as win32 # pip install pywin32

    xlsx_path.parent.mkdir(parents=True, exist_ok=True)

    excel = win32.gencache.EnsureDispatch("Excel.Application")
    excel.Visible = False 
    excel.DisplayAlerts = False # evita prompts que travariam o script

    try:
        wb = excel.Workbooks.Open(str(xls_path))
        # FileFormat=51 => .xlsx (Excel Workbook)
        wb.SaveAs(str(xlsx_path), FileFormat=51)
        wb.Close()
    finally:
        excel.Quit()

# -----------------------------
# 3) Pipeline one-shot
# -----------------------------

def is_legacy_xls(file_name: str, mime_type: str) -> bool:
    """
    Identifica arquivos Excel legados (.xls).

    Referências:
    - https://developers.google.com/drive/api/guides/mime-types
    - https://developers.google.com/drive/api/reference/rest/v3/files
    """
    name = file_name.lower()
    return (
        mime_type == "application/vnd.ms-excel"
        or name.endswith(".xls")
    )


def run_one_shot_convert(
    input_folder_id=LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID,
    output_folder_id=LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID,
    max_files=500
) -> None:
    """
    Varre o Drive, baixa .xls, converte e sobe .xlsx.
    """
    # Preparar pastas temporárias
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    files = list_drive_files(input_folder_id, max_files=max_files)
    print(f"[INFO] Encontrados {len(files)} arquivos na pasta de entrda.")

    converted = 0
    skipped = 0 
    failed = 0

    for f in files:
        file_id = f["id"]
        name = f["name"]
        mime_type = f.get("mimeType", "")

        if not is_legacy_xls(name, mime_type):
            skipped += 1
            continue 
        print(f"\n[INFO] Processando legado: {name} ({file_id})")

        # 1) Baixar
        local_xls = RAW_DIR / name 
        try: 
            download_drive_file(file_id, local_xls)
            print(f"[OK] Baixado: {local_xls}")
        except Exception as e:
            failed += 1
            print(f"[ERRO] Falha ao baixar {name}: {e} ")
            continue 
        
        # 2) Converter
        local_xlsx = OUT_DIR / (local_xls.stem + ".xlsx")
        try: 
            convert_xls_to_xlsx_excel_com(local_xls, local_xlsx)
            print(f"[OK] Convertido: {local_xlsx}")
        except Exception as e:
            failed += 1
            print(f"[ERRO] Falha ao converter {name}: {e}")
            continue 

        # 3) Upload 
        try:
            new_id = upload_xlsx_to_drive(local_xlsx, output_folder_id)
            print(f"[OK] Enviado para Drive: {local_xlsx.name} (id={new_id})") 
            converted += 1 
        except Exception as e: 
            failed += 1
            print(f"[ERRO] Falha ao enviar {local_xlsx.name}: {e}")

    print("\n======================")
    print(f"[RESUMO] Convertidos: {converted}")
    print(f"[RESUMO] Ignorados (não .xls): {skipped}")
    print(f"[RESUMO] Falhas: {failed}")
    print(f"[RESUMO] Pasta RAW: {RAW_DIR}")
    print(f"[RESUMO] Pasta OUT: {OUT_DIR}")

if __name__ == "__main__":
    if not LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID or not LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID:
        raise ValueError("Defina GOOGLE_DRIVE_INPUT_FOLDER_ID e GOOGLE_DRIVE_OUTPUT_FOLDER_ID no .env")

    run_one_shot_convert(
        input_folder_id=LEGACY_GOOGLE_DRIVE_INPUT_FOLDER_ID,
        output_folder_id=LEGACY_GOOGLE_DRIVE_OUTPUT_FOLDER_ID,
        max_files=500
    )



