from __future__ import annotations

import re
import unicodedata
import pandas as pd


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    return df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", case=False, regex=True)]


def _slugify(name: str) -> str:
    s = str(name).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("utf-8")
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_slugify(c) for c in df.columns]
    return df


def rename_known_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    rename_map = {
        "almox": "almox",
        "endereco": "endereco",
        "item": "item_id",
        "descricao": "descricao",
        "qtd_wms": "qtd_wms",
        "qtd_fisico": "qtd_fisico",
        "qtd_dif": "qtd_dif",
        "data_contagem": "data_contagem",
        "qtd_erp": "qtd_erp",
        "valor_dif": "valor_dif",
        "status": "status",
        "motivo": "motivo",
        "id": "id",
    }
    return df.rename(columns={c: rename_map[c] for c in df.columns if c in rename_map})


def coerce_datetime(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df = df.copy()
    if col not in df.columns:
        raise ValueError(f"Coluna de data '{col}' não encontrada. Colunas: {list(df.columns)}")
    df[col] = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
    return df[df[col].notna()]


def smart_to_float(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "None": None, "nan": None, "NaN": None})

    mask_both = s.str.contains(r"\.", na=False) & s.str.contains(r",", na=False)
    s.loc[mask_both] = (
        s.loc[mask_both]
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    mask_comma = (~mask_both) & s.str.contains(",", na=False)
    s.loc[mask_comma] = s.loc[mask_comma].str.replace(",", ".", regex=False)

    return pd.to_numeric(s, errors="coerce")


def coerce_numeric_ptbr(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = smart_to_float(df[c])
    return df


def filter_last_months(df: pd.DataFrame, date_col: str, months: int = 12) -> pd.DataFrame:
    """
    Janela 12 meses robusta:
    usa a MAIOR data do próprio dataset como referência (não o 'agora' do PC).
    """
    df = df.copy()
    if df.empty:
        return df
    ref = df[date_col].max()
    cutoff = ref.normalize() - pd.DateOffset(months=months)
    return df[df[date_col] >= cutoff]


def transform_inventory(df_raw: pd.DataFrame):
    report = {}
    df = df_raw.copy()

    report["linhas_iniciais"] = len(df)

    df = drop_unnamed_columns(df)
    df = normalize_column_names(df)
    df = rename_known_columns(df)
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    report["apos_normalizacao"] = len(df)

    # Datas
    before = len(df)
    df["data_contagem_raw"] = df.get("data_contagem")
    df = coerce_datetime(df, "data_contagem")
    report["removidas_data_invalida"] = before - len(df)

    # Janela 12 meses
    before = len(df)
    df = filter_last_months(df, "data_contagem", months=12)
    report["removidas_fora_janela"] = before - len(df)

    report["linhas_finais"] = len(df)

    df = coerce_numeric_ptbr(df, ["qtd_wms", "qtd_fisico", "qtd_dif", "qtd_erp", "valor_dif"])

    return df, report
