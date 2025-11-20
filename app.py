import streamlit as st
from invent2 import run_pipeline

st.set_page_config(page_title="Atualização Inventário BV", layout="wide")

st.title("Atualização da base de inventário (12 meses)")
st.write(
    """
    Este app atualiza a base consolidada de inventário a partir dos arquivos
    diários no Google Drive e grava o resultado na planilha do Google Sheets
    configurada no `.env` / `SHEET_OUTPUT_ID`.
    """
)

if st.button("🚀 Atualizar base agora"):
    with st.spinner("Processando arquivos de inventário..."):
        resumo = run_pipeline()

    # proteção extra: se por algum motivo vier None
    if resumo is None:
        st.error("O pipeline não retornou resumo. Veja os logs do servidor.")
    elif resumo["arquivos_processados"] == 0:
        st.error("Nenhum arquivo válido foi processado. Verifique os logs.")
    else:
        st.success("Atualização concluída com sucesso!")

        col1, col2, col3 = st.columns(3)
        col1.metric("Arquivos encontrados", resumo["arquivos_encontrados"])
        col2.metric("Arquivos processados", resumo["arquivos_processados"])
        col3.metric("Linhas finais na base", resumo["linhas_finais"])

        if resumo["data_min"] is not None:
            st.write(
                f"Período coberto: **{resumo['data_min'].date()}** "
                f"até **{resumo['data_max'].date()}**"
            )

        with st.expander("Ver amostra da base consolidada"):
            st.dataframe(resumo["df_final"].head(100))
else:
    st.info("Clique em **🚀 Atualizar base agora** para rodar o pipeline.")
