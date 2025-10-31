# main.py - VERSÃO SERVIDOR WEB COMPLETA

import os
import sys
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import threading

# Importações para a API do Google
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Importações para o Servidor Web
from flask import Flask, jsonify

# --- 1. CONFIGURAÇÕES E CONSTANTES ---
# Esta seção não muda.
load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1wTu1axBrOiaQkDr8H06md5vr9BmDkaAtilNaoh03Ddw'
TARGET_SHEET_NAME = 'REDS_TCO_CONFERENCIA'
START_CELL = 'C3'
FILTER_RANGE = 'A2:K2'
SQL_FILE_PATH = os.path.join('sql_scripts', 'registros_tco_2025.sql')
GOOGLE_CREDS_PATH = os.path.join('config', 'credentials.json')
GOOGLE_TOKEN_PATH = os.path.join('config', 'token.json')

# --- INICIALIZAÇÃO DO SERVIDOR FLASK ---
app = Flask(__name__)
# ATENÇÃO: Mude esta chave para algo único e secreto!
SECRET_KEY = os.getenv('CHAVE_SECRETA')
update_status = {"running": False, "last_run": "Nunca"}

# --- 2. LÓGICA DE ATUALIZAÇÃO (AGORA DENTRO DE FUNÇÕES) ---
# Todas as nossas funções de trabalho são colocadas aqui.


def fetch_data_from_bisp(sql_query, user, pwd):
    """Conecta-se ao Impala/BISP, executa uma consulta e retorna um DataFrame."""
    print("\n[BISP] Iniciando conexão com o banco de dados...")
    connection_string = (
        f"Driver={{Cloudera ODBC Driver for Impala}};"
        f"Host=dlmg.prodemge.gov.br;Port=21051;AuthMech=3;"
        f"UID={user};PWD={pwd};"
        f"TransportMode=sasl;KrbServiceName=impala;SSL=1;"
        f"AllowSelfSignedServerCert=1;AutoReconnect=1;UseSQLUnicode=1;"
        f"TrustedCerts=config/cacerts.pem;"  # <--- ADICIONE ESTA LINHA
    )
    try:
        # A biblioteca pandas pode gerenciar a conexão diretamente.
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            print("   - [BISP] Conexão estabelecida com sucesso.")
            df = pd.read_sql(sql_query, conn)
            print(
                f"   - [BISP] Consulta executada. {len(df)} registros encontrados.")
            return df
    except Exception as e:
        print(f"\n[ERRO BISP] Falha ao buscar dados: {e}")
        return None


def get_google_sheets_service():
    """Autentica com a API do Google e retorna um objeto de serviço."""
    print("\n[GOOGLE] Iniciando autenticação com a API...")
    creds = None
    if os.path.exists(GOOGLE_TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(
            GOOGLE_TOKEN_PATH, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Esta parte não funcionará bem dentro do Docker sem interação manual.
            # É ESSENCIAL que você rode o script localmente uma vez para gerar o token.json.
            print("[ATENÇÃO] O arquivo token.json não foi encontrado ou é inválido.")
            print(
                "Execute o script fora do Docker uma vez para gerar o token.json na pasta 'config'.")
            return None
        with open(GOOGLE_TOKEN_PATH, 'w') as token:
            token.write(creds.to_json())
    print("   - [GOOGLE] Serviço autenticado com sucesso.")
    return build('sheets', 'v4', credentials=creds)


def _get_sheet_id(service, sheet_name):
    """Busca o ID numérico de uma aba pelo seu nome."""
    sheet_metadata = service.spreadsheets().get(
        spreadsheetId=SPREADSHEET_ID).execute()
    for sheet in sheet_metadata.get('sheets', ''):
        if sheet.get('properties', {}).get('title', '') == sheet_name:
            return sheet.get('properties', {}).get('sheetId', 0)
    return None


def update_google_sheet(service, df):
    """Função principal que orquestra a atualização da planilha."""
    print("\n[SHEETS] Iniciando processo de atualização da planilha...")
    sheet_id = _get_sheet_id(service, TARGET_SHEET_NAME)
    if sheet_id is None:
        print(f"[ERRO SHEETS] A aba '{TARGET_SHEET_NAME}' não foi encontrada.")
        return False

    # 1. Remover filtro
    print("   - [SHEETS] Removendo filtro existente...")
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body={
        "requests": [{"clearBasicFilter": {"sheetId": sheet_id}}]}).execute()

    # 2. Limpar dados antigos
    clear_range = f"{TARGET_SHEET_NAME}!C3:Z"
    print(f"   - [SHEETS] Limpando dados antigos no intervalo: {clear_range}")
    service.spreadsheets().values().clear(
        spreadsheetId=SPREADSHEET_ID, range=clear_range).execute()

    # 3. Formatar dados
    print("   - [SHEETS] Formatando os dados...")
    colunas_data = ['data_hora_fato', 'data_hora_fechamento']
    colunas_enviadas = ['numero_ocorrencia', 'data_hora_fato', 'nome_municipio', 'natureza_codigo',
                        'natureza_descricao', 'ind_tco', 'relator_matricula', 'relator_nome', 'data_hora_fechamento']
    df_formatado = df[colunas_enviadas].copy()
    for col in df_formatado.columns:
        if col in colunas_data:
            df_formatado[col] = pd.to_datetime(
                df_formatado[col], errors='coerce').dt.strftime('%d/%m/%Y')
        else:
            df_formatado[col] = df_formatado[col].astype(str)

    # 4. Inserir novos dados
    data_to_insert = df_formatado.fillna('').values.tolist()
    print(f"   - [SHEETS] Inserindo {len(data_to_insert)} novas linhas...")
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=f"{TARGET_SHEET_NAME}!{START_CELL}",
        valueInputOption='USER_ENTERED', body={'values': data_to_insert}
    ).execute()

    # 5. Recriar filtro
    print("   - [SHEETS] Recriando o filtro...")
    from_row, to_row = int(FILTER_RANGE.split(
        ':')[0][1:]) - 1, int(FILTER_RANGE.split(':')[1][1:])
    from_col, to_col = ord(FILTER_RANGE.split(':')[0][0].upper(
    )) - ord('A'), ord(FILTER_RANGE.split(':')[1][0].upper()) - ord('A') + 1
    requests = [{"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": from_row,
                                                         "endRowIndex": to_row, "startColumnIndex": from_col, "endColumnIndex": to_col}}}}]
    service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID,
                                       body={"requests": requests}).execute()

    print("\n[SUCESSO] Planilha atualizada com sucesso! ✅")
    return True


def run_the_full_update_task():
    """Função 'mãe' que executa todo o processo em segundo plano."""
    global update_status
    update_status["running"] = True
    print("\n" + "="*50)
    print(
        f"INICIANDO TAREFA DE ATUALIZAÇÃO EM BACKGROUND - {datetime.now().strftime('%H:%M:%S')}")
    print("="*50)

    try:
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        data_df = fetch_data_from_bisp(sql_query, DB_USER, DB_PASSWORD)

        if data_df is not None and not data_df.empty:
            sheets_service = get_google_sheets_service()
            if sheets_service:
                update_google_sheet(sheets_service, data_df)
                update_status["last_run"] = datetime.now().strftime(
                    '%d/%m/%Y %H:%M:%S')
        else:
            print(
                "[AVISO] Nenhum dado foi retornado do banco. Nenhuma atualização foi feita na planilha.")

    except Exception as e:
        print(f"\n[ERRO GERAL NA TAREFA] Ocorreu uma falha inesperada: {e}")

    finally:
        print("\n" + "="*50)
        print(
            f"TAREFA DE ATUALIZAÇÃO FINALIZADA - {datetime.now().strftime('%H:%M:%S')}")
        print("="*50)
        update_status["running"] = False

# --- 3. ROTAS DA API (ENDPOINTS) ---


@app.route("/")
def index():
    """Página inicial para verificar o status do servidor."""
    status_text = "em andamento" if update_status['running'] else "ocioso"
    return (
        f"<h1>Servidor de Automação TCO está no ar!</h1>"
        f"<p>Status: <strong>{status_text}</strong></p>"
        f"<p>Última execução bem-sucedida: {update_status['last_run']}</p>"
    )


@app.route(f"/run-update/<string:key>")
def trigger_update(key):
    """Endpoint que dispara a atualização, protegido por uma chave secreta."""
    if key != SECRET_KEY:
        return jsonify({"status": "error", "message": "Chave secreta inválida."}), 403

    if update_status["running"]:
        return jsonify({"status": "error", "message": "Uma atualização já está em andamento."}), 409

    update_thread = threading.Thread(target=run_the_full_update_task)
    update_thread.start()

    return jsonify({"status": "success", "message": "Processo de atualização iniciado em segundo plano."})


# --- 4. EXECUÇÃO DO SERVIDOR ---
if __name__ == '__main__':
    print("--- SERVIDOR DE AUTOMAÇÃO TCO ---")
    print(
        f"Para disparar a atualização, acesse: http://localhost:5000/run-update/{SECRET_KEY}")
    print("Use o ngrok para expor esta porta para a internet.")
    app.run(host='0.0.0.0', port=5000, debug=False)
