# main.py
import os, re, datetime as dt
import pandas as pd
from loguru import logger
from utils import list_gsheets_in_folder, read_gsheet_to_df, write_output

PASTA_ID = os.getenv("DRIVE_FOLDER_INPUT")
HIST_SOURCE = os.getenv("HIST_SOURCE")   # caminho do CSV histórico ou ID da planilha destino
DESTINO = os.getenv("DESTINO")           # 'sheets' ou 'bigquery'

COLS_MAP = {
    'qtd._erp?': 'qtd_erp',
    'qtd._wms?': 'qtd_wms',
    'qtd._dif.?': 'qtd_dif',
    'valor_dif.?': 'valor_dif',
    'status': 'status',
    'id': 'id',
    'item': 'item_id',
    'descrição|descricao': 'descricao',
    'motivo': 'motivo'
}

def normalize_cols(df):
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.normalize('NFKD')
                  .str.encode('ascii','ignore').str.decode('utf-8')
    )
    out = {}
    for c in df.columns:
        std = None
        for pat, tgt in COLS_MAP.items():
            if re.fullmatch(pat, c):
                std = tgt; break
        out[c] = std or c
    df = df.rename(columns=out)
    return df

def parse_date_store(filename):
    # WF0041_inventario_2025-11-09_loja-RRP.gsheet
    m = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
    d = pd.to_datetime(m.group(1)).date() if m else dt.date.today()
    loja = None
    m2 = re.search(r'loja-([A-Za-z0-9_-]+)', filename)
    if m2: loja = m2.group(1).upper()
    return d, loja

def transform(df, dt_inv, loja):
    df = normalize_cols(df)
    df['dt_inventario'] = pd.to_datetime(dt_inv)
    if 'qtd_erp' in df: df['qtd_erp'] = pd.to_numeric(df['qtd_erp'], errors='coerce').fillna(0)
    if 'qtd_wms' in df: df['qtd_wms'] = pd.to_numeric(df['qtd_wms'], errors='coerce').fillna(0)
    df['qtd_dif_calc'] = df['qtd_wms'] - df['qtd_erp']
    if 'qtd_dif' in df:
        df['flag_inconsistencia'] = (df['qtd_dif'].astype(float).round(2) != df['qtd_dif_calc'].round(2))
    df['qtd_dif'] = df['qtd_dif_calc']
    df['loja'] = loja
    df['ingestion_ts'] = pd.Timestamp.utcnow()
    return df[['dt_inventario','id','item_id','descricao','status',
               'qtd_erp','qtd_wms','qtd_dif','valor_dif','motivo',
               'loja','ingestion_ts']]

def main():
    logger.info("Início da ingestão")
    frames = []
    for file in list_gsheets_in_folder(PASTA_ID):
        dt_inv, loja = parse_date_store(file['name'])
        df_raw = read_gsheet_to_df(file['id'])
        frames.append(transform(df_raw, dt_inv, loja))

    df = pd.concat(frames, ignore_index=True)
    # carregar histórico (se existir) e concatenar
    hist = pd.DataFrame()
    if os.path.exists(HIST_SOURCE):
        hist = pd.read_csv(HIST_SOURCE, parse_dates=['dt_inventario','ingestion_ts'])
    df_all = pd.concat([hist, df], ignore_index=True)

    # deduplicação pela chave
    df_all = (df_all.sort_values('ingestion_ts')
                     .drop_duplicates(['dt_inventario','loja','id','item_id'], keep='last'))

    # janela 12 meses
    cutoff = pd.Timestamp.utcnow().normalize() - pd.DateOffset(months=12)
    df_all = df_all[df_all['dt_inventario'] >= cutoff]

    # métricas úteis
    df_all['dif_abs'] = df_all['qtd_dif'].abs()
    df_all['acuracia'] = 1 - (df_all['dif_abs'] / df_all['qtd_wms'].replace({0:pd.NA}))
    df_all['acuracia'] = df_all['acuracia'].clip(lower=0, upper=1)

    write_output(df_all, DESTINO)
    logger.info(f"Linhas finais: {len(df_all)}")

if __name__ == "__main__":
    main()
