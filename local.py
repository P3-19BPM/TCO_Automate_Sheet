# local.py - VERSÃO PARA EXECUÇÃO LOCAL (MANUAL)
# MODIFICADO PARA USAR CONTA DE SERVIÇO (ROBÔ)

import os
import sys
import pyodbc
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

# Importações para a API do Google (usando Conta de Serviço)
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# --- 1. CONFIGURAÇÕES E CONSTANTES ---
load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# Escopos necessários para a API do Sheets e do Drive
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

SPREADSHEET_ID = '1wTu1axBrOiaQkDr8H06md5vr9BmDkaAtilNaoh03Ddw'
TARGET_SHEET_NAME = 'REDS_TCO_CONFERENCIA'
SQL_FILE_PATH = os.path.join('sql_scripts', 'registros_tco_2025.sql')

# CAMINHO PARA O NOVO ARQUIVO JSON DA CONTA DE SERVIÇO
# Certifique-se de que o nome do arquivo corresponde ao que você baixou
# <-- AJUSTE AQUI SE O NOME FOR DIFERENTE
SERVICE_ACCOUNT_FILE = os.path.join('config', 'credenciais-robo.json')


# --- 2. LÓGICA DE ATUALIZAÇÃO ---

def fetch_data_from_bisp(sql_query, user, pwd):
    """Conecta-se ao Impala/BISP, executa uma consulta e retorna um DataFrame."""
    print("\n[BISP] Iniciando conexão com o banco de dados...")
    connection_string = (
        f"Driver={{Cloudera ODBC Driver for Impala}};"
        f"Host=dlmg.prodemge.gov.br;Port=21051;AuthMech=3;"
        f"UID={user};PWD={pwd};"
        f"TransportMode=sasl;KrbServiceName=impala;SSL=1;"
        f"AllowSelfSignedServerCert=1;AutoReconnect=1;UseSQLUnicode=1;"
        f"TrustedCerts=config/cacerts.pem;"
    )
    try:
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            print("   - [BISP] Conexão estabelecida com sucesso.")
            df = pd.read_sql(sql_query, conn)
            print(
                f"   - [BISP] Consulta executada. {len(df)} registros encontrados.")
            return df
    except Exception as e:
        print(f"\n[ERRO BISP] Falha ao buscar dados: {e}")
        return None


def get_gspread_client():
    """Autentica com a API do Google usando uma Conta de Serviço e retorna um cliente gspread."""
    print("\n[GOOGLE] Iniciando autenticação com a API (via Conta de Serviço)...")
    try:
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        print("   - [GOOGLE] Cliente autenticado com sucesso.")
        return client
    except FileNotFoundError:
        print(
            f"[ERRO GOOGLE] Arquivo de credenciais não encontrado em: '{SERVICE_ACCOUNT_FILE}'")
        print("   - Verifique se o nome do arquivo e o caminho estão corretos.")
        return None
    except Exception as e:
        print(f"[ERRO GOOGLE] Falha na autenticação: {e}")
        return None


def update_google_sheet(client, df):
    """Função principal que orquestra a atualização da planilha usando gspread."""
    print("\n[SHEETS] Iniciando processo de atualização da planilha...")
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        sheet_id = worksheet.id  # Pega o ID numérico da aba, necessário para o batchUpdate
        print(
            f"   - [SHEETS] Aba '{TARGET_SHEET_NAME}' (ID: {sheet_id}) acessada com sucesso.")

        print(f"   - [SHEETS] Limpando dados antigos a partir da célula C3...")
        if worksheet.row_count > 2:
            worksheet.clear_basic_filter()
            # Limpa apenas a área de dados para preservar outros formatos
            range_to_clear = f'C3:{gspread.utils.rowcol_to_a1(worksheet.row_count, worksheet.col_count)}'
            worksheet.batch_clear([range_to_clear])

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
        df_formatado.fillna('', inplace=True)

        print(f"   - [SHEETS] Inserindo {len(df_formatado)} novas linhas...")
        worksheet.update(range_name='C3', values=df_formatado.values.tolist(
        ), value_input_option='USER_ENTERED')

        print("   - [SHEETS] Recriando o filtro e formatando a célula A1...")

        # --- LÓGICA DE FORMATAÇÃO AVANÇADA RESTAURADA ---
        # Vamos construir um único pedido 'batchUpdate' para o filtro e a célula A1

        # 1. Pedido para recriar o filtro
        filter_request = {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,  # Linha 2
                        "endRowIndex": worksheet.row_count,
                        "startColumnIndex": 0,  # Coluna A
                        "endColumnIndex": 11  # Coluna K
                    }
                }
            }
        }

        # 2. Pedido para atualizar e formatar a célula A1 (lógica do seu script original)
        today_date_str = datetime.now().strftime('%d/%m/%Y')
        text_part1 = "ATUALIZADO\n"
        text_part2 = today_date_str
        start_index_part2 = len(text_part1)

        black_color = {"red": 0, "green": 0, "blue": 0}
        red_color = {"red": 1, "green": 0, "blue": 0}

        update_cell_a1_request = {
            "updateCells": {
                "rows": [{
                    "values": [{
                        "userEnteredValue": {"stringValue": f"{text_part1}{text_part2}"},
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER",
                            "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP"
                        },
                        "textFormatRuns": [
                            {
                                "startIndex": 0,
                                "format": {"bold": True, "foregroundColor": black_color, "fontSize": 18}
                            },
                            {
                                "startIndex": start_index_part2,
                                "format": {"bold": True, "foregroundColor": red_color, "fontSize": 18}
                            }
                        ]
                    }]
                }],
                "fields": "userEnteredValue,userEnteredFormat,textFormatRuns",
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0, "endRowIndex": 1,  # Célula A1
                    "startColumnIndex": 0, "endColumnIndex": 1
                }
            }
        }

        # Envia os dois pedidos (filtro e formatação da célula A1) de uma só vez
        spreadsheet.batch_update(
            body={"requests": [filter_request, update_cell_a1_request]})

        print("\n[SUCESSO] Planilha atualizada com sucesso! ✅")
        return True

    except gspread.exceptions.WorksheetNotFound:
        print(f"[ERRO SHEETS] A aba '{TARGET_SHEET_NAME}' não foi encontrada.")
        return False
    except Exception as e:
        print(
            f"[ERRO SHEETS] Ocorreu uma falha durante a atualização da planilha: {e}")
        return False


def run_local_update():
    """Função 'mãe' que executa todo o processo."""
    print("\n" + "="*50)
    print(
        f"INICIANDO TAREFA DE ATUALIZAÇÃO LOCAL - {datetime.now().strftime('%H:%M:%S')}")
    print("="*50)

    try:
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        data_df = fetch_data_from_bisp(sql_query, DB_USER, DB_PASSWORD)

        if data_df is not None and not data_df.empty:
            gspread_client = get_gspread_client()
            if gspread_client:
                update_google_sheet(gspread_client, data_df)
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


# --- 3. EXECUÇÃO DO SCRIPT ---
if __name__ == '__main__':
    # Antes de rodar, instale as bibliotecas necessárias:
    # pip install gspread google-auth-oauthlib google-auth-httplib2 pandas pyodbc python-dotenv
    run_local_update()
