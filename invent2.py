import os
import re
import datetime as dt
from pathlib import Path

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
    creds_path = Path("credentials/indicadores-inventario-bv.json")
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

# Carrega .env (com override para garantir atualização)
load_dotenv(override=True)

PASTA_ID = os.getenv("DRIVE_FOLDER_INPUT")
HIST_SOURCE = os.getenv("HIST_SOURCE")
DESTINO = os.getenv("DESTINO")


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
        ano = 2000 + ano2
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


def transform(df: pd.DataFrame, dt_inv, loja: str) -> pd.DataFrame:
    """
    - Normaliza nomes de colunas
    - Garante presença de 'qtd_erp' e 'qtd_wms'
    - Converte colunas numéricas
    - Recalcula qtd_dif
    - Adiciona dt_inventario, loja, ingestion_ts
    """
    df = normalize_cols(df)

    required = ["qtd_erp", "qtd_wms"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes após normalização: {missing}. "
            f"dt_inv={dt_inv}, loja={loja}. "
            f"Colunas encontradas: {list(df.columns)}"
        )

    df["dt_inventario"] = pd.to_datetime(dt_inv)

    df["qtd_erp"] = pd.to_numeric(df["qtd_erp"], errors="coerce").fillna(0)
    df["qtd_wms"] = pd.to_numeric(df["qtd_wms"], errors="coerce").fillna(0)

    df["qtd_dif_calc"] = df["qtd_wms"] - df["qtd_erp"]

    if "qtd_dif" in df.columns:
        df["qtd_dif"] = pd.to_numeric(df["qtd_dif"], errors="coerce")
        df["flag_inconsistencia"] = (
            df["qtd_dif"].round(2) != df["qtd_dif_calc"].round(2)
        )

    df["qtd_dif"] = df["qtd_dif_calc"]

    if "valor_dif" in df.columns:
        df["valor_dif"] = pd.to_numeric(df["valor_dif"], errors="coerce")
    else:
        df["valor_dif"] = pd.NA

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


def run_pipeline():
    """Roda o pipeline de inventário e devolve um resumo para o Streamlit."""
    print(f"[DEBUG] PASTA_ID: {PASTA_ID}")
    print(f"[DEBUG] DESTINO: {DESTINO}")
    print(f"[DEBUG] HIST_SOURCE: {HIST_SOURCE}")
    
    if not PASTA_ID:
        raise RuntimeError("DRIVE_FOLDER_INPUT não definido no .env")

    logger.info("Início da ingestão")

    # PASSO 1: Processar todos os arquivos da pasta Drive
    frames = []
    files = list_gsheets_in_folder(PASTA_ID)
    logger.info(f"Arquivos encontrados na pasta de entrada: {len(files)}")

    for file in files:
        nome = file["name"]
        file_id = file["id"]

        try:
            dt_inv, loja = parse_date_store(nome)
            df_raw = read_gsheet_to_df(file_id)
            
            # DEBUG: Mostrar quantas linhas cada arquivo tem
            logger.info(f"Arquivo {nome}: {len(df_raw)} linhas brutas")
            
            df_tr = transform(df_raw, dt_inv, loja)
            
            logger.info(f"Arquivo {nome}: {len(df_tr)} linhas após transformação")
            
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

    # PASSO 2: Juntar APENAS os arquivos novos processados
    df_novos = pd.concat(frames, ignore_index=True)
    logger.info(f"Total de linhas dos arquivos novos: {len(df_novos)}")

    # PASSO 3: Ler histórico SOMENTE para manter dados antigos (fora da janela de 12 meses dos novos)
    hist = pd.DataFrame()
    if HIST_SOURCE:
        hist_path = Path(HIST_SOURCE)
        if hist_path.exists():
            try:
                hist = pd.read_csv(
                    hist_path,
                    parse_dates=["dt_inventario", "ingestion_ts"],
                )
                logger.info(f"Lido histórico local: {len(hist)} linhas")
                
                # CRÍTICO: Remover do histórico as datas que estão nos arquivos novos
                # para evitar duplicação
                datas_novas = df_novos["dt_inventario"].unique()
                hist = hist[~hist["dt_inventario"].isin(datas_novas)]
                logger.info(f"Histórico após remover datas duplicadas: {len(hist)} linhas")
                
            except Exception as e:
                logger.warning(f"Falha ao ler histórico local: {e}. Ignorando.")
        else:
            logger.info("Arquivo histórico não encontrado. Iniciando com base vazia.")

    # PASSO 4: Concatenar histórico limpo + dados novos
    df_all = pd.concat([hist, df_novos], ignore_index=True)
    logger.info(f"Total após concatenação: {len(df_all)} linhas")

    # PASSO 5: Deduplicação pela chave (por segurança)
    subset = ["dt_inventario", "loja", "item_id"]
    if "id" in df_all.columns:
        subset.insert(1, "id")

    linhas_antes_dedup = len(df_all)
    df_all = (
        df_all.sort_values("ingestion_ts")
              .drop_duplicates(subset=subset, keep="last")
    )
    logger.info(f"Deduplicação removeu {linhas_antes_dedup - len(df_all)} linhas")

    # PASSO 6: Aplicar janela móvel de 12 meses
    df_all["dt_inventario"] = pd.to_datetime(df_all["dt_inventario"]).dt.tz_localize(None)
    cutoff = pd.Timestamp.now().normalize() - pd.DateOffset(months=12)
    
    linhas_antes_janela = len(df_all)
    df_all = df_all[df_all["dt_inventario"] >= cutoff]
    logger.info(f"Janela de 12 meses removeu {linhas_antes_janela - len(df_all)} linhas")

    # PASSO 7: Calcular métricas
    df_all["dif_abs"] = df_all["qtd_dif"].abs()
    df_all["acuracia"] = 1 - (
        df_all["dif_abs"] / df_all["qtd_wms"].replace({0: pd.NA})
    )
    df_all["acuracia"] = df_all["acuracia"].clip(lower=0, upper=1)

    # PASSO 8: Escrever resultado
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