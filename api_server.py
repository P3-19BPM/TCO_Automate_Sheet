from flask import Flask, jsonify, render_template, request
from datetime import datetime, timezone
import threading
import time
import os
import pyodbc
import pandas as pd
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import json
import numpy as np

# --- Configurações ---
load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
SPREADSHEET_ID = '1wTu1axBrOiaQkDr8H06md5vr9BmDkaAtilNaoh03Ddw'
TARGET_SHEET_NAME = 'REDS_TCO_CONFERENCIA'
SERVICE_ACCOUNT_FILE = os.path.join('config', 'credenciais-robo.json')
SQL_FILE_PATH = os.path.join('sql_scripts', 'registros_tco_2025.sql')
SQL_DIAGNOSTICO_PATH = os.path.join('sql_scripts', 'diagnostico_tco.sql')
SQL_ATUALIZACAO_PATH = os.path.join('sql_scripts', 'atualizacao.sql')
CACHE_FILE_PATH = os.path.join('cache', 'diagnostico_cache.csv')
CACHE_STATUS_PATH = os.path.join('cache', 'cache_status.json')

os.makedirs('cache', exist_ok=True)

# --- FUNÇÕES COMPLETAS (SEM OMISSÕES) ---


def fetch_data_from_bisp(sql_query, user, pwd):
    print(f"\n[BISP] Executando consulta...")
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


def update_google_sheet(client, df):
    print("\n[SHEETS] Iniciando processo de atualização da planilha...")
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        sheet_id = worksheet.id
        print(
            f"   - [SHEETS] Aba '{TARGET_SHEET_NAME}' (ID: {sheet_id}) acessada com sucesso.")
        if worksheet.row_count > 2:
            worksheet.clear_basic_filter()
            range_to_clear = f'C3:{gspread.utils.rowcol_to_a1(worksheet.row_count, worksheet.col_count)}'
            worksheet.batch_clear([range_to_clear])
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
        worksheet.update(range_name='C3', values=df_formatado.values.tolist(
        ), value_input_option='USER_ENTERED')
        filter_request = {"setBasicFilter": {"filter": {"range": {"sheetId": sheet_id, "startRowIndex": 1,
                                                                  "endRowIndex": worksheet.row_count, "startColumnIndex": 0, "endColumnIndex": 11}}}}
        today_date_str = datetime.now().strftime('%d/%m/%Y')
        text_part1 = "ATUALIZADO\n"
        start_index_part2 = len(text_part1)
        update_cell_a1_request = {"updateCells": {"rows": [{"values": [{"userEnteredValue": {"stringValue": f"{text_part1}{today_date_str}"}, "userEnteredFormat": {"horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP"}, "textFormatRuns": [{"startIndex": 0, "format": {"bold": True, "foregroundColor": {
            "red": 0, "green": 0, "blue": 0}, "fontSize": 18}}, {"startIndex": start_index_part2, "format": {"bold": True, "foregroundColor": {"red": 1, "green": 0, "blue": 0}, "fontSize": 18}}]}]}], "fields": "userEnteredValue,userEnteredFormat,textFormatRuns", "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 1}}}
        spreadsheet.batch_update(
            body={"requests": [filter_request, update_cell_a1_request]})
        return True, "Planilha atualizada com sucesso!"
    except Exception as e:
        error_message = f"Falha na atualização do Sheets: {e}"
        print(f"[ERRO SHEETS] {error_message}")
        return False, error_message


process_status = {
    "status": "Pronto para iniciar",
    "timestamp": datetime.now().isoformat()
}
status_lock = threading.Lock()


def set_status(new_status):
    with status_lock:
        process_status["status"] = new_status
        process_status["timestamp"] = datetime.now().isoformat()
    print(f"[STATUS] {new_status}")


def run_full_update_process():
    try:
        set_status("Iniciando processo...")
        time.sleep(2)
        set_status("Lendo o script SQL...")
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        set_status(
            "Conectando ao banco de dados e buscando dados (pode levar um tempo)...")
        data_df = fetch_data_from_bisp(sql_query, DB_USER, DB_PASSWORD)
        if data_df is None or data_df.empty:
            set_status(
                "AVISO: Nenhum dado retornado do banco. O processo será finalizado.")
            time.sleep(10)
            set_status("Pronto para iniciar")
            return
        set_status("Autenticando com a API do Google...")
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        if not client:
            set_status(
                "ERRO: Falha ao autenticar com Google. Verifique os logs do servidor.")
            return
        set_status(f"Atualizando a planilha com {len(data_df)} registros...")
        success, message = update_google_sheet(client, data_df)
        if success:
            set_status("SUCESSO! Planilha atualizada. ✅")
        else:
            set_status(f"ERRO: {message}")
    except Exception as e:
        error_msg = f"ERRO GERAL: Uma falha inesperada ocorreu. Verifique os logs. ({e})"
        set_status(error_msg)
    finally:
        time.sleep(20)
        set_status("Pronto para iniciar")

# --- FIM DAS FUNÇÕES COMPLETAS ---


app = Flask(__name__)

# --- ROTAS PARA ATUALIZAÇÃO DA PLANILHA ---


@app.route('/')
def index():
    return render_template('status.html')


@app.route('/iniciar-atualizacao', methods=['POST'])
def trigger_update():
    with status_lock:
        if "Pronto" not in process_status["status"]:
            return jsonify({"status": "error", "message": "Um processo já está em andamento."}), 409
    print("Requisição para iniciar a atualização recebida.")
    thread = threading.Thread(target=run_full_update_process)
    thread.start()
    return jsonify({"status": "success", "message": "Processo de atualização iniciado."})


@app.route('/status')
def get_status():
    with status_lock:
        return jsonify(process_status)

# --- ROTAS PARA O PAINEL DE DIAGNÓSTICO ---


@app.route('/diagnostico')
def painel_diagnostico():
    return render_template('diagnostico.html')


def get_dataframe_from_cache_or_bisp():
    """Função centralizada para obter o DataFrame, usando o sistema de cache."""
    cache_info = {"source": "desconhecida", "last_updated": None}

    with open(SQL_ATUALIZACAO_PATH, 'r', encoding='utf-8') as f:
        sql_check_query = f.read()

    last_bisp_update_df = fetch_data_from_bisp(
        sql_check_query, DB_USER, DB_PASSWORD)
    if last_bisp_update_df is None:
        raise Exception(
            "Falha ao conectar ao BISP para verificar a data de atualização. Verifique a conexão ou as credenciais.")
    if last_bisp_update_df.empty:
        raise Exception(
            "A consulta de data de atualização do BISP não retornou resultados.")

    last_bisp_update_str = last_bisp_update_df.iloc[0, 0]
    last_bisp_update = pd.to_datetime(last_bisp_update_str).tz_localize('UTC')

    last_cache_update = None
    if os.path.exists(CACHE_STATUS_PATH):
        with open(CACHE_STATUS_PATH, 'r') as f:
            cache_status = json.load(f)
            last_cache_update = datetime.fromisoformat(
                cache_status['last_updated']).replace(tzinfo=timezone.utc)

    if not last_cache_update or last_bisp_update > last_cache_update:
        print("   - [CACHE] Cache desatualizado. Buscando novos dados do BISP...")
        cache_info["source"] = "BISP (Atualizado Agora)"
        with open(SQL_DIAGNOSTICO_PATH, 'r', encoding='utf-8') as f:
            sql_diagnostico_query = f.read()

        df = fetch_data_from_bisp(sql_diagnostico_query, DB_USER, DB_PASSWORD)
        if df is None:
            raise Exception(
                "Falha ao buscar os dados de diagnóstico do BISP. A consulta principal falhou.")

        df.to_csv(CACHE_FILE_PATH, index=False)
        now_utc = datetime.now(timezone.utc)
        with open(CACHE_STATUS_PATH, 'w') as f:
            json.dump({'last_updated': now_utc.isoformat()}, f)
        cache_info["last_updated"] = now_utc.isoformat()
    else:
        print("   - [CACHE] Cache está atualizado. Lendo dados do arquivo local.")
        cache_info["source"] = "Cache Local"
        cache_info["last_updated"] = last_cache_update.isoformat()
        if not os.path.exists(CACHE_FILE_PATH):
            raise Exception(
                f"Arquivo de cache '{CACHE_FILE_PATH}' não encontrado.")
        df = pd.read_csv(CACHE_FILE_PATH)

    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    if 'registro_válido' in df.columns:
        df['registro_válido'] = df['registro_válido'].astype(
            str).str.lower().isin(['true', '1', 't'])

    return df, cache_info


@app.route('/api/dados-diagnostico')
def get_dados_diagnostico():
    try:
        df, cache_info = get_dataframe_from_cache_or_bisp()

        coluna_local = "local_fato"
        coluna_valido = "registro_válido"
        coluna_motivo = "motivos_caso_inválido"

        metas = {
            'BARRAGEM TODOS OS SANTOS': 15,
            'CEAM - BARRAGEM TODOS OS SANTOS': 15,
            'AGÊNCIA DE ATENDIMENTO': 10,
            'ALMOXARIFADO SÃO JOSÉ': 10
        }

        if coluna_local not in df.columns or coluna_valido not in df.columns:
            raise KeyError(
                f"Uma das colunas essenciais ('{coluna_local}' ou '{coluna_valido}') não foi encontrada nos dados após a normalização.")

        stats_df = df.groupby(coluna_local)[coluna_valido].agg(
            VÁLIDOS=lambda x: x.sum(),
            INVÁLIDOS=lambda x: (~x).sum()
        ).reset_index()
        stats_df['META'] = stats_df[coluna_local].map(metas)
        stats_df['% CUMPRIMENTO'] = (
            stats_df['VÁLIDOS'] / stats_df['META'] * 100).fillna(0).round(2)
        stats_df['RESTANTES'] = stats_df['META'] - stats_df['VÁLIDOS']
        stats_df = stats_df.rename(columns={coluna_local: 'ENDEREÇO'})
        stats_json = stats_df[['ENDEREÇO', 'META', 'VÁLIDOS', 'INVÁLIDOS',
                               '% CUMPRIMENTO', 'RESTANTES']].to_dict(orient='records')

        filtro_local_val = request.args.get('filtro_local', '')
        filtro_motivo_val = request.args.get('filtro_motivo', '').lower()
        filtro_cia = request.args.get('filtro_cia', '')
        filtro_valido = request.args.get('filtro_valido', '')
        sort_by = request.args.get('sort_by', '').lower().replace(' ', '_')
        sort_order = request.args.get('sort_order', 'asc')

        df_filtrado = df.copy()

        locais_cia = {
            "42": ["BARRAGEM TODOS OS SANTOS", "CEAM - BARRAGEM TODOS OS SANTOS"],
            "47": ["AGÊNCIA DE ATENDIMENTO", "ALMOXARIFADO SÃO JOSÉ"]
        }
        if filtro_cia and filtro_cia in locais_cia:
            df_filtrado = df_filtrado[df_filtrado[coluna_local].isin(
                locais_cia[filtro_cia])]

        if filtro_local_val:
            df_filtrado = df_filtrado[df_filtrado[coluna_local]
                                      == filtro_local_val]

        if filtro_valido == 'validos':
            df_filtrado = df_filtrado[df_filtrado[coluna_valido] == True]
        elif filtro_valido == 'invalidos':
            df_filtrado = df_filtrado[df_filtrado[coluna_valido] == False]

        if filtro_motivo_val and coluna_motivo in df_filtrado.columns:
            df_filtrado = df_filtrado[df_filtrado[coluna_motivo].astype(
                str).str.lower().str.contains(filtro_motivo_val, na=False)]

        if sort_by and sort_by in df_filtrado.columns:
            df_filtrado = df_filtrado.sort_values(
                by=sort_by, ascending=(sort_order == 'asc'), na_position='last')

        colunas_para_exibir_normalizadas = [
            'bos', 'hora_fato', 'local_fato', 'histórico_ocorrência', 'motivos_caso_inválido', 'registro_válido'
        ]
        df_final = df_filtrado[[
            col for col in colunas_para_exibir_normalizadas if col in df_filtrado.columns]]

        df_final.columns = [col.replace('_', ' ').upper()
                            for col in df_final.columns]

        df_final = df_final.replace({np.nan: None})
        detalhes_json = df_final.to_dict(orient='records')

        return jsonify({
            "stats": stats_json,
            "details": detalhes_json,
            "cache_info": cache_info
        })

    except Exception as e:
        print(f"[ERRO DIAGNÓSTICO] {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    print("Servidor de automação iniciado.")
    print("Para a atualização da planilha, acesse de outra máquina na rede:")
    print("http://10.14.56.62:8088/")
    print("\nNOVO: Para o painel de diagnóstico, acesse:")
    print("http://10.14.56.62:8088/diagnostico")
    app.run(host='0.0.0.0', port=8088, debug=True)
