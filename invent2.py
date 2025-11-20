import os
import re
import datetime as dt

import pandas as pd
from loguru import logger
from dotenv import load_dotenv

from utils import list_gsheets_in_folder, read_gsheet_to_df, write_output

# Detecta se está no ambiente do Streamlit Cloud
IS_STREAMLIT_CLOUD = os.getenv("STREAMLIT_RUNTIME") is not None

if IS_STREAMLIT_CLOUD:
    import streamlit as st

    # 1) Recupera o JSON das credenciais a partir dos secrets
    creds_json = st.secrets["GOOGLE_CREDENTIALS"]

    # 2) Garante que a pasta credentials existe
    os.makedirs("credentials", exist_ok=True)
    creds_path = pathlib.Path("credentials/indicadores-inventario-bv.json")
    creds_path.write_text(creds_json, encoding="utf-8")

    # 3) Ajusta a variável de ambiente para o caminho do arquivo
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(creds_path)

    # 4) Copia as outras variáveis dos secrets para o ambiente
    for key in [
        "DRIVE_FOLDER_INPUT",
        "DRIVE_FOLDER_OUTPUT",
        "SHEET_OUTPUT_ID",
        "HIST_SOURCE",
        "DESTINO",
    ]:
        if key in st.secrets:
            os.environ[key] = st.secrets[key]
else:
    # Ambiente local: carrega .env normalmente
    load_dotenv()

# Carrega .env
load_dotenv(override=True)

PASTA_ID = os.getenv("DRIVE_FOLDER_INPUT")
HIST_SOURCE = os.getenv("HIST_SOURCE")   # CSV histórico local
DESTINO = os.getenv("DESTINO")           # 'sheets' ou outro


# ---------------------------------------------------------------------
# NORMALIZAÇÃO DE COLUNAS
# ---------------------------------------------------------------------
def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normaliza nomes de colunas:
    - tira espaços extras
    - coloca em minúsculas
    - remove acentos
    - troca qualquer coisa que não seja [a-z0-9] por "_"
    - remove "_" do começo/fim
    - renomeia "item" -> "item_id"
    """
    df = df.copy()

    df.columns = (
        df.columns
          .str.strip()
          .str.lower()
          .str.normalize('NFKD')
          .str.encode('ascii', 'ignore').str.decode('utf-8')
          .str.replace(r'[^a-z0-9]+', '_', regex=True)
          .str.strip('_')
    )

    rename_map = {
        "item": "item_id",
    }
    df = df.rename(columns=rename_map)

    return df


# ---------------------------------------------------------------------
# PARSE DA DATA E LOJA A PARTIR DO NOME DO ARQUIVO
# ---------------------------------------------------------------------
def parse_date_store(filename: str):
    """
    Extrai data a partir do nome do arquivo no formato:
        ContagemDDMMYY.xlsx

    Ex.: Contagem031125.xlsx -> 2025-11-03

    Se não achar, usa a data de hoje.
    Loja ainda é opcional (pode ser estendida depois).
    """
    m = re.search(r"(\d{2})(\d{2})(\d{2})", filename)
    if m:
        dia, mes, ano2 = map(int, m.groups())
        ano = 2000 + ano2  # assume século 21
        try:
            d = dt.date(ano, mes, dia)
        except ValueError:
            d = dt.date.today()
    else:
        d = dt.date.today()

    loja = None
    m2 = re.search(r"loja-([A-Za-z0-9_-]+)", filename, flags=re.IGNORECASE)
    if m2:
        loja = m2.group(1).upper()

    return d, loja


# ---------------------------------------------------------------------
# TRANSFORMAÇÃO DO DATAFRAME BRUTO EM FORMATO PADRÃO
# ---------------------------------------------------------------------
def transform(df: pd.DataFrame, dt_inv, loja: str) -> pd.DataFrame:
    """
    - Normaliza nomes de colunas
    - Garante presença de 'qtd_erp' e 'qtd_wms'
    - Converte colunas numéricas
    - Recalcula qtd_dif
    - Adiciona dt_inventario, loja, ingestion_ts
    """
    df = normalize_cols(df)

    # Valida colunas obrigatórias
    required = ["qtd_erp", "qtd_wms"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes após normalização: {missing}. "
            f"dt_inv={dt_inv}, loja={loja}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df["dt_inventario"] = pd.to_datetime(dt_inv)

    # Numéricos
    df["qtd_erp"] = pd.to_numeric(df["qtd_erp"], errors="coerce").fillna(0)
    df["qtd_wms"] = pd.to_numeric(df["qtd_wms"], errors="coerce").fillna(0)

    # Diferença calculada
    df["qtd_dif_calc"] = df["qtd_wms"] - df["qtd_erp"]

    # Se houver qtd_dif original, marcamos inconsistência
    if "qtd_dif" in df.columns:
        df["qtd_dif"] = pd.to_numeric(df["qtd_dif"], errors="coerce")
        df["flag_inconsistencia"] = (
            df["qtd_dif"].round(2) != df["qtd_dif_calc"].round(2)
        )

    # Em qualquer caso, passamos a usar a calculada
    df["qtd_dif"] = df["qtd_dif_calc"]

    # Valor dif opcional
    if "valor_dif" in df.columns:
        df["valor_dif"] = pd.to_numeric(df["valor_dif"], errors="coerce")
    else:
        df["valor_dif"] = pd.NA

    # Garantir colunas de texto existindo
    if "status" not in df.columns:
        df["status"] = pd.NA
    if "descricao" not in df.columns:
        df["descricao"] = pd.NA
    if "motivo" not in df.columns:
        df["motivo"] = pd.NA

    df["loja"] = loja
    df["ingestion_ts"] = pd.Timestamp.now()

    cols_final = [
        "dt_inventario",
        "id" if "id" in df.columns else None,
        "item_id",
        "descricao",
        "status",
        "qtd_erp",
        "qtd_wms",
        "qtd_dif",
        "valor_dif",
        "motivo",
        "loja",
        "ingestion_ts",
    ]
    cols_final = [c for c in cols_final if c is not None]

    return df[cols_final]


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main():
    if not PASTA_ID:
        raise RuntimeError("DRIVE_FOLDER_INPUT não definido no .env")

    logger.info("Início da ingestão")

    frames = []
    files = list_gsheets_in_folder(PASTA_ID)
    logger.info(f"Arquivos encontrados na pasta de entrada: {len(files)}")

    for file in files:
        nome = file["name"]
        file_id = file["id"]

        try:
            dt_inv, loja = parse_date_store(nome)
            df_raw = read_gsheet_to_df(file_id)
            df_tr = transform(df_raw, dt_inv, loja)
            frames.append(df_tr)
            logger.info(f"Arquivo processado: {nome} ({file_id})")
        except Exception as e:
            logger.error(f"Falha ao processar arquivo {nome} ({file_id}): {e}")

    if not frames:
        logger.warning("Nenhum arquivo válido foi processado. Encerrando.")
        return

    df = pd.concat(frames, ignore_index=True)

    # Histórico local
    hist = pd.DataFrame()
    if HIST_SOURCE and os.path.exists(HIST_SOURCE):
        logger.info(f"Lendo histórico local de {HIST_SOURCE}")
        hist = pd.read_csv(
            HIST_SOURCE,
            parse_dates=["dt_inventario", "ingestion_ts"],
        )

    df_all = pd.concat([hist, df], ignore_index=True)

    # Deduplicação pela chave
    subset = ["dt_inventario", "loja", "item_id"]
    if "id" in df_all.columns:
        subset.insert(1, "id")  # dt_inventario, id, loja, item_id

    df_all = (
        df_all.sort_values("ingestion_ts")
              .drop_duplicates(subset=subset, keep="last")
    )

    # Janela móvel de 12 meses
    df_all["dt_inventario"] = pd.to_datetime(df_all["dt_inventario"]).dt.tz_localize(None)

    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=12)
    df_all = df_all[df_all["dt_inventario"] >= cutoff]

    # Métricas de qualidade
    df_all["dif_abs"] = df_all["qtd_dif"].abs()
    df_all["acuracia"] = 1 - (
        df_all["dif_abs"] / df_all["qtd_wms"].replace({0: pd.NA})
    )
    df_all["acuracia"] = df_all["acuracia"].clip(lower=0, upper=1)

    # Escrita no destino
    write_output(df_all, DESTINO or "sheets")

    logger.info(f"Linhas finais na base consolidada: {len(df_all)}")
    logger.info("Processo concluído com sucesso.")


def run_pipeline():
    """Roda o pipeline de inventário e devolve um resumo para o Streamlit."""
    print(f"[DEBUG] PASTA_ID: {PASTA_ID}")
    print(f"[DEBUG] DESTINO: {DESTINO}")
    print(f"[DEBUG] HIST_SOURCE: {HIST_SOURCE}")
    if not PASTA_ID:
        raise RuntimeError("DRIVE_FOLDER_INPUT não definido no .env")

    logger.info("Início da ingestão")

    frames = []
    files = list_gsheets_in_folder(PASTA_ID)
    logger.info(f"Arquivos encontrados na pasta de entrada: {len(files)}")

    for file in files:
        nome = file["name"]
        file_id = file["id"]

        try:
            dt_inv, loja = parse_date_store(nome)
            df_raw = read_gsheet_to_df(file_id)
            df_tr = transform(df_raw, dt_inv, loja)
            frames.append(df_tr)
            logger.info(f"Arquivo processado: {nome} ({file_id})")
        except Exception as e:
            logger.error(f"Falha ao processar arquivo {nome} ({file_id}): {e}")

    if not frames:
        logger.warning("Nenhum arquivo válido foi processado.")
        return {
            "arquivos_encontrados": len(files),
            "arquivos_processados": 0,
            "linhas_finais": 0,
            "data_min": None,
            "data_max": None,
            "df_final": pd.DataFrame(),
        }

    # Junta tudo que veio dos arquivos da pasta
    df = pd.concat(frames, ignore_index=True)

    # Histórico local, se existir
    hist = pd.DataFrame()
    if HIST_SOURCE and os.path.exists(HIST_SOURCE):
        logger.info(f"Lendo histórico local de {HIST_SOURCE}")
        hist = pd.read_csv(
            HIST_SOURCE,
            parse_dates=["dt_inventario", "ingestion_ts"],
        )

    df_all = pd.concat([hist, df], ignore_index=True)

    # Deduplicação pela chave
    subset = ["dt_inventario", "loja", "item_id"]
    if "id" in df_all.columns:
        subset.insert(1, "id")  # dt_inventario, id, loja, item_id

    df_all = (
        df_all.sort_values("ingestion_ts")
              .drop_duplicates(subset=subset, keep="last")
    )

    # Janela móvel de 12 meses (timezone-naive)
    df_all["dt_inventario"] = pd.to_datetime(df_all["dt_inventario"]).dt.tz_localize(None)
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=12)
    df_all = df_all[df_all["dt_inventario"] >= cutoff]

    # Métricas
    df_all["dif_abs"] = df_all["qtd_dif"].abs()
    df_all["acuracia"] = 1 - (
        df_all["dif_abs"] / df_all["qtd_wms"].replace({0: pd.NA})
    )
    df_all["acuracia"] = df_all["acuracia"].clip(lower=0, upper=1)

    # Escreve no destino (Google Sheets + CSV histórico)
    write_output(df_all, DESTINO or "sheets")

    resumo = {
        "arquivos_encontrados": len(files),
        "arquivos_processados": len(frames),
        "linhas_finais": len(df_all),
        "data_min": df_all["dt_inventario"].min(),
        "data_max": df_all["dt_inventario"].max(),
        "df_final": df_all,
    }

    logger.info(f"Linhas finais na base consolidada: {len(df_all)}")
    logger.info("Processo concluído com sucesso.")

    return resumo


def main():
    """Entrada quando você roda `python invent2.py` no terminal."""
    run_pipeline()


if __name__ == "__main__":
    main()