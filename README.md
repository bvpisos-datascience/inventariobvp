## 1️⃣ Título + Resumo do Projeto

**Função:** deixar claro *em uma linha* o que o projeto faz.

> ETL Google Sheets → Google Drive → Looker Studio> 
> Pipeline automatizado para consolidar arquivos Google Sheets, tratá-los e disponibilizar um dataset final para dashboards.

## 2️⃣ Arquitetura Geral do Projeto

**Função:** mostrar visualmente como as peças se conectam.
Google Drive (pasta de entrada)
      ↓
   Leitura Sheets
      ↓
 Consolidação Pandas (motor)
      ↓
 Tratamentos / Normalizações
      ↓
 Arquivo final CSV/Sheet
      ↓
Google Drive (pasta de saída)
      ↓
Looker Studio

## 3️⃣ Estrutura de Pastas (tree visual)

**Função:** explicar a organização modular.

C:.
│   .env
│   .gitignore
│   app.py
│   invent.py
│   invent2.py
│   README.md
│   requirements.txt
│   teste.ipynb
│   utils.py
│
├───.devcontainer
│       devcontainer.json
│
├───base_diaria
├───credentials
│       indicadores-inventario-bv.json
│
├───data
│   ├───processed_wms
│   └───raw_wms
├───lixo
├───notebooks
├───src
│   ├───app
│   └───core
├───_dict
├───_logs
├───_outputs
│       base_inventario_12meses.csv
│
└───__pycache__
        invent2.cpython-312.pyc
        utils.cpython-312.pyc

## 4️⃣ Instalação e Setup do Ambiente

**Função:** garantir que QUALQUER pessoa consiga rodar o projeto mesmo sem te perguntar nada.

Criando o ambiente:
python -m inventario .venv 
source inventario/bin/activate # Linux/Mac
.\.venv\Scripts\activate # windows
pip install -r requirements.txt

Configurando variáveis no .env:
GOOGLE_SERVICE_ACCOUNT_FILE=service_account.json
GOOGLE_DRIVE_INPUT_FOLDER_ID=XXXXXXXX
GOOGLE_DRIVE_OUTPUT_FOLDER_ID=YYYYYYYY

## 5️⃣ Como executar o projeto (motor + UI)

**Função:** instrução objetiva, sem enrolação.

python -m src.core.ingestion

Rodar apenas o Streamlit:
streamlit run src/app/main_app.py

## 6️⃣ Requisitos funcionais do projeto

**Função:** explicar o que o sistema deve FAZER.

- Ler automaticamente todos os arquivos Google Sheets de uma pasta.
- Validar se possuem a mesma estrutura.
- Consolidar em um único DataFrame.
- Aplicar regras de transformação (tipos, normalização, limpeza).
- Gerar CSV final.
- Fazer upload para pasta de saída.
- Expor botão de execução via Streamlit.

## 7️⃣ Tecnologias utilizadas

**Função:** mostrar maturidade técnica e contexto das escolhas.

- Python 3.12.12
- Pandas
- Streamlit
- Google Drive API
- Google Sheets API
- gspread
- python-dotenv

## 8️⃣ Boas práticas implementadas

**Função:** mostrar nível profissional.

- Separação de camadas (`src/core` vs `src/app`).
- Não subir credenciais para o Git.
- Uso de `.env`.
- Modularização em funções reutilizáveis.
- Logs e mensagens de erro amigáveis.

## Roadmap e futuras evoluções

9️⃣ Aqui você demonstra visão de longo prazo, por exemplo:

- Agendar execução diária no Cloud Run.
- Reescrever partes críticas com testes unitários.
- Versionar datasets usando DVC.
- Criar interface multi-pastas no Streamlit.