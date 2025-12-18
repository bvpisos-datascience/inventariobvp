import pandas as pd
from pathlib import Path

from .output import write_csv_local, upload_csv_to_drive, update_google_sheet
from .ingestion import load_all_files_as_dataframes, consolidate_dataframes
from .transform import transform_inventory


def run_pipeline(max_files: int = 450) -> pd.DataFrame:
    """
    Orquestra:
    - Fase 3: ingestão
    - Fase 4: transformação
    - Fase 5: escrita local + upload Drive + Google Sheets
    """
    BASE_DIR = Path(__file__).resolve().parents[2]

    # ---- Fase 3: ingestão
    dfs = load_all_files_as_dataframes(max_files=max_files)
    print(f"[PIPELINE] Arquivos lidos (dataframes): {len(dfs)}")

    df_raw = consolidate_dataframes(dfs)
    print(f"[PIPELINE] DF raw: {df_raw.shape[0]} linhas, {df_raw.shape[1]} colunas")

    # ---- Fase 4: transformação
    df_final, report = transform_inventory(df_raw)
    print("[AUDIT]", report)

    # Sanidade do filtro 12 meses (prova)
    if "data_contagem" in df_final.columns and not df_final.empty:
        print(
            "[PIPELINE] data_contagem min/max:",
            df_final["data_contagem"].min(),
            df_final["data_contagem"].max(),
        )

    # ---- Fase 5: escrita + upload
    csv_path = write_csv_local(df_final, BASE_DIR)
    drive_id = upload_csv_to_drive(csv_path)
    print(f"[PIPELINE] Upload OK. Drive file id = {drive_id}")
    
    # ← ADICIONE ESTAS LINHAS:
    update_google_sheet(df_final)
    print("[PIPELINE] Google Sheet atualizada com sucesso")

    return df_final


if __name__ == "__main__":
    df = run_pipeline(max_files=450)
    print(df.head(5))