# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime, timezone
from werkzeug.utils import secure_filename
from flask import Flask, jsonify, render_template, request, send_file
from pypdf import PdfReader, PdfWriter

import io
import os
import json
import time
import threading
import shutil
import subprocess
import numpy as np
import pandas as pd
import pyodbc
import gspread
import re
import unicodedata

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# =========================
# CONFIGURAÇÕES GERAIS
# =========================
load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']
SPREADSHEET_ID = '1wTu1axBrOiaQkDr8H06md5vr9BmDkaAtilNaoh03Ddw'
TARGET_SHEET_NAME = 'REDS_TCO_CONFERENCIA'
SERVICE_ACCOUNT_FILE = os.path.join('config', 'credenciais-robo.json')

SQL_FILE_PATH = os.path.join('sql_scripts', 'registros_tco_2025.sql')
SQL_DIAGNOSTICO_PATH = os.path.join('sql_scripts', 'diagnostico_COPASA.sql')
SQL_ATUALIZACAO_PATH = os.path.join('sql_scripts', 'atualizacao.sql')

CACHE_FILE_PATH = os.path.join('cache', 'diagnostico_cache.csv')
CACHE_STATUS_PATH = os.path.join('cache', 'cache_status.json')
AUDIT_FILE_PATH = os.path.join(
    'cache', 'auditoria_copasa.json')  # <-- ADICIONE ESTA LINHA

os.makedirs('cache', exist_ok=True)

app = Flask(__name__, static_folder="static")
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200 MB

# Registrar Blueprint CEMIG ISOLADO
from cemig import cemig_bp
app.register_blueprint(cemig_bp)

# =========================
# BISP + SHEETS (sem alterações de lógica)
# =========================


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
    print("\n[SHEETS] Iniciando atualização da planilha...")
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        worksheet = spreadsheet.worksheet(TARGET_SHEET_NAME)
        sheet_id = worksheet.id

        if worksheet.row_count > 2:
            worksheet.clear_basic_filter()
            rng = f'C3:{gspread.utils.rowcol_to_a1(worksheet.row_count, worksheet.col_count)}'
            worksheet.batch_clear([rng])

        colunas_data = ['data_hora_fato', 'data_hora_fechamento']
        colunas_enviadas = [
            'numero_ocorrencia', 'data_hora_fato', 'nome_municipio', 'natureza_codigo',
            'natureza_descricao', 'ind_tco', 'relator_matricula', 'relator_nome',
            'data_hora_fechamento'
        ]

        df_formatado = df[colunas_enviadas].copy()
        for col in df_formatado.columns:
            if col in colunas_data:
                df_formatado[col] = pd.to_datetime(df_formatado[col], errors='coerce')\
                    .dt.strftime('%d/%m/%Y')
            else:
                df_formatado[col] = df_formatado[col].astype(str)
        df_formatado.fillna('', inplace=True)

        worksheet.update(range_name='C3',
                         values=df_formatado.values.tolist(),
                         value_input_option='USER_ENTERED')

        # A1 com "ATUALIZADO"
        today_date_str = datetime.now().strftime('%d/%m/%Y')
        text_part1 = "ATUALIZADO\n"
        start_index_part2 = len(text_part1)
        filter_request = {"setBasicFilter": {"filter": {"range": {
            "sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": worksheet.row_count,
            "startColumnIndex": 0, "endColumnIndex": 11}}}}
        update_cell_a1_request = {
            "updateCells": {
                "rows": [{
                    "values": [{
                        "userEnteredValue": {"stringValue": f"{text_part1}{today_date_str}"},
                        "userEnteredFormat": {
                            "horizontalAlignment": "CENTER", "verticalAlignment": "MIDDLE",
                            "wrapStrategy": "WRAP"
                        },
                        "textFormatRuns": [
                            {"startIndex": 0,
                             "format": {"bold": True, "foregroundColor": {"red": 0, "green": 0, "blue": 0}, "fontSize": 18}},
                            {"startIndex": start_index_part2,
                             "format": {"bold": True, "foregroundColor": {"red": 1, "green": 0, "blue": 0}, "fontSize": 18}}
                        ]
                    }]
                }],
                "fields": "userEnteredValue,userEnteredFormat,textFormatRuns",
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": 0, "endColumnIndex": 1}
            }
        }
        spreadsheet.batch_update(
            body={"requests": [filter_request, update_cell_a1_request]})
        return True, "Planilha atualizada com sucesso!"
    except Exception as e:
        return False, f"Falha na atualização do Sheets: {e}"


process_status = {"status": "Pronto para iniciar",
                  "timestamp": datetime.now().isoformat()}
status_lock = threading.Lock()


def set_status(new_status):
    with status_lock:
        process_status["status"] = new_status
        process_status["timestamp"] = datetime.now().isoformat()
    print(f"[STATUS] {new_status}")


def run_full_update_process():
    try:
        set_status("Iniciando processo...")
        time.sleep(1.5)
        set_status("Lendo o script SQL...")
        with open(SQL_FILE_PATH, 'r', encoding='utf-8') as f:
            sql_query = f.read()

        set_status("Conectando ao banco e buscando dados...")
        data_df = fetch_data_from_bisp(sql_query, DB_USER, DB_PASSWORD)
        if data_df is None or data_df.empty:
            set_status("AVISO: Nenhum dado retornado do banco.")
            time.sleep(4)
            set_status("Pronto para iniciar")
            return

        set_status("Autenticando com a API do Google...")
        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)

        set_status(f"Atualizando a planilha ({len(data_df)} registros)...")
        ok, msg = update_google_sheet(client, data_df)
        set_status("SUCESSO! Planilha atualizada. ✅" if ok else f"ERRO: {msg}")
    except Exception as e:
        set_status(f"ERRO GERAL: {e}")
    finally:
        time.sleep(3)
        set_status("Pronto para iniciar")

# =========================
# ROTAS BÁSICAS
# =========================


@app.route('/api/copasa/export-xlsx')
def export_xlsx_validos():
    try:
        # Carrega o mesmo DF do painel
        df, _ = get_dataframe_from_cache_or_bisp()

        # Normaliza nome da coluna (por segurança)
        df.columns = [c.lower().replace(" ", "_") for c in df.columns]

        # Filtro do mês vindo do front
        filtro_mes = request.args.get('filtro_mes', '').strip()
        if filtro_mes:
            mm, yyyy = filtro_mes.split('-')
            df = df[(df['mês'] == int(mm)) & (df['ano'] == int(yyyy))]

        # MAPEIA LOCAL EM ORDEM
        mapa = {
            'BARRAGEM TODOS OS SANTOS': '01 - BARRAGEM TODOS OS SANTOS',
            'CEAM - BARRAGEM TODOS OS SANTOS': '02 - CEAM - BARRAGEM TODOS OS SANTOS',
            'AGÊNCIA DE ATENDIMENTO': '03 - AGÊNCIA DE ATENDIMENTO',
            'ALMOXARIFADO SÃO JOSÉ': '04 - ALMOXARIFADO SÃO JOSÉ'
        }

        df['local_fato_registro'] = df['local_fato'].map(
            mapa).fillna('99 - OUTROS')

        # ================================
        # ABA 1 – CONSOLIDADO (SÓ VÁLIDOS)
        # ================================
        # Normaliza coluna registro_válido
        df['registro_válido'] = df['registro_válido'].astype(
            str).str.lower().isin(['true', '1', 't'])

        df_validos = df[df['registro_válido'] == True].copy()

        consolidado = (
            df_validos.groupby(['local_fato_registro', 'mês'], as_index=False)
            .agg(
                quantidade_registros=('bos', pd.Series.nunique),
                **{'Nº BOS/RAT': ('bos', lambda s: '; '.join(sorted(set(s.tolist()))))}
            )
        )

        # Ordenação CASE
        ordem = [
            '01 - BARRAGEM TODOS OS SANTOS',
            '02 - CEAM - BARRAGEM TODOS OS SANTOS',
            '03 - AGÊNCIA DE ATENDIMENTO',
            '04 - ALMOXARIFADO SÃO JOSÉ',
            '99 - OUTROS'
        ]
        consolidado['__ord'] = pd.Categorical(
            consolidado['local_fato_registro'], categories=ordem, ordered=True)
        consolidado = consolidado.sort_values(
            ['__ord', 'mês']).drop(columns='__ord')

        # ================================
        # ABA 2 – DETALHADO (TODOS)
        # ================================
        df['STATUS'] = df['registro_válido'].apply(
            lambda x: 'Válido' if x else 'Inválido')

        detalhado = df[['local_fato_registro', 'mês', 'STATUS', 'bos']].copy()
        detalhado = detalhado.sort_values(
            ['local_fato_registro', 'mês', 'STATUS', 'bos'])

        # ================================
        # EXPORTA PARA XLSX COM 2 ABAS
        # ================================
        output_path = Path("cache/relatorio_validos_COPASA.xlsx")

        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            consolidado.to_excel(writer, sheet_name='Consolidado', index=False)
            detalhado.to_excel(writer, sheet_name='Detalhado', index=False)

        return send_file(output_path, as_attachment=True, download_name="relatorio_COPASA.xlsx")

    except Exception as e:
        print(f"[ERRO EXPORT XLSX] {e}")
        return jsonify({"error": str(e)}), 500


# =========================
# GHOSTSCRIPT COMPRESSÃO (FORÇADA E EFICIENTE)
# =========================
GS_BIN = os.getenv("GS_BIN", "gswin64c")  # ou "gswin32c" se for 32-bit


def _gs_bin():
    """Verifica se Ghostscript está no PATH ou usa GS_BIN"""
    return GS_BIN if shutil.which(GS_BIN) else None


def compress_pdf_gs(input_pdf: Path, output_pdf: Path, quality: str = "ebook"):
    gs = _gs_bin()
    if not gs:
        raise RuntimeError(
            "Ghostscript não encontrado. Instale ou defina GS_BIN no .env")

    cmd = [
        gs, "-q", "-dNOPAUSE", "-dBATCH", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",  # fallback
        f"-sOutputFile={output_pdf}", str(input_pdf)
    ]

    # === PARÂMETROS PERSONALIZADOS ===
    if quality == "screen":
        cmd = cmd[:-2] + [
            "-dColorImageResolution=72",
            "-dGrayImageResolution=72",
            "-dMonoImageResolution=72",
            "-dDownsampleColorImages=true",
            "-dDownsampleGrayImages=true",
            "-dDownsampleMonoImages=true",
            "-dColorImageDownsampleType=/Bicubic",
            "-dGrayImageDownsampleType=/Bicubic",
            "-dMonoImageDownsampleType=/Bicubic",
            "-dEmbedAllFonts=false",
            "-dSubsetFonts=true",
            "-dAutoFilterColorImages=true",
            "-dAutoFilterGrayImages=true",
            "-dColorImageFilter=/FlateEncode",
            "-dGrayImageFilter=/FlateEncode"
        ] + cmd[-2:]

    elif quality == "ebook":
        cmd = cmd[:-2] + [
            "-dColorImageResolution=150",
            "-dGrayImageResolution=150",
            "-dDownsampleColorImages=true",
            "-dDownsampleGrayImages=true",
            "-dColorImageDownsampleType=/Bicubic"
        ] + cmd[-2:]

    elif quality == "printer":
        cmd = cmd[:-2] + [
            "-dColorImageResolution=300",
            "-dGrayImageResolution=300"
        ] + cmd[-2:]

    print(f"[GS] {input_pdf.name} → {quality}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"Ghostscript falhou: {result.stderr[:200]}")

    if not output_pdf.exists() or output_pdf.stat().st_size == 0:
        raise RuntimeError("Arquivo de saída vazio.")


@app.route('/')
def index():
    return render_template('status.html')


@app.route('/iniciar-atualizacao', methods=['POST'])
def trigger_update():
    with status_lock:
        if "Pronto" not in process_status["status"]:
            return jsonify({"status": "error", "message": "Um processo já está em andamento."}), 409
    threading.Thread(target=run_full_update_process).start()
    return jsonify({"status": "success", "message": "Processo de atualização iniciado."})


@app.route('/status')
def get_status():
    with status_lock:
        return jsonify(process_status)


@app.route('/diagnostico')
def painel_diagnostico():
    return render_template('diagnostico.html')


@app.route('/copasa', endpoint='copasa')
def painel_diagnostico_copasa():
    return render_template('copasa.html')

# =========================
# PROCESSAMENTO DE DADOS (PANDAS)
# =========================


def processar_dados_python(df_raw):
    """
    Recebe o DF bruto do SQL e aplica toda a lógica de validação via Pandas.
    Versão Ajustada: Inferência de Município pelo Local.
    """
    print("   - [PYTHON] Iniciando processamento e validação de dados...")

    if df_raw is None or df_raw.empty:
        return pd.DataFrame()

    # --- 1. Normalização de Texto ---
    def normalizar(text):
        if not isinstance(text, str):
            return ""
        try:
            # Remove acentos
            text = unicodedata.normalize('NFKD', text).encode(
                'ASCII', 'ignore').decode('utf-8')
        except:
            pass
        text = text.lower()
        # Remove pontuação (troca por espaço)
        text = re.sub(r'[^a-z0-9]', ' ', text)
        # Remove espaços múltiplos
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    # Garante colunas de texto (converte para string antes de normalizar para evitar erros)
    df_raw['h_norm'] = df_raw['historico_ocorrencia'].astype(
        str).apply(normalizar)

    if 'nome_completo_envolvido' in df_raw.columns:
        df_raw['nome_norm'] = df_raw['nome_completo_envolvido'].astype(
            str).apply(normalizar)
    else:
        df_raw['nome_norm'] = ""

    # --- 2. Definição de Locais ---
    def definir_local(cod):
        # Remove .0 caso venha como float (ex: 3361.0 -> 3361)
        cod = str(cod).replace('.0', '').strip()
        if cod == '3361':
            return 'AGÊNCIA DE ATENDIMENTO'
        if cod == '2601':
            return 'ALMOXARIFADO SÃO JOSÉ'
        if cod in ['-498661', '-515062', '-497640', '-558382']:
            return 'CEAM - BARRAGEM TODOS OS SANTOS'
        if cod in ['-570056', '-497579']:
            return 'BARRAGEM TODOS OS SANTOS'
        return 'OUTROS'

    # Tenta pegar a coluna de código do logradouro (pode variar o nome)
    col_logradouro = 'logradouro_codigo'
    if 'codigo_logradouro' in df_raw.columns:
        col_logradouro = 'codigo_logradouro'

    # Aplica a definição de local
    if col_logradouro in df_raw.columns:
        df_raw['local_fato_calc'] = df_raw[col_logradouro].apply(definir_local)
    else:
        df_raw['local_fato_calc'] = 'OUTROS'

    resultados = []

    # --- 3. Loop de Validação ---
    for idx, row in df_raw.iterrows():
        hist = row['h_norm']
        nome_full_norm = row['nome_norm']
        nome_original = row.get('nome_completo_envolvido')
        local_identificado = row['local_fato_calc']

        # --- Flags de Texto ---
        f_unle = any(x in hist for x in ['unle', 'unidade de negocio'])
        f_unidade = any(x in hist for x in [
                        'copasa', 'ete', 'eta', 'barragem', 'reservat', 'elevat', 'almoxarifado', 'agencia'])

        keywords_func = [
            'funcionario', 'funcionário', 'FUNCIONÁRIO', 'funcionarios', 'colaborador', 'colaboradores',
            'vigilante', 'vigilantes', 'superviso', 'supervisor',
            'atendente', 'contato com', 'contactado', 'guarnicao', 'sr ', 'sra '
        ]
        f_func = any(k in hist for k in keywords_func)
        f_end = any(x in hist for x in [
                    'logradouro', 'coord', 'lat', 'long', 'bairro', 'rua', 'municipio'])

        # --- LÓGICA MUNICÍPIO (CORREÇÃO) ---
        f_municipio = False
        raw_mun = str(row.get('codigo_municipio', ''))

        # SE O LOCAL FOI IDENTIFICADO (NÃO É 'OUTROS'), ASSUME-SE QUE O MUNICÍPIO ESTÁ CERTO.
        if local_identificado != 'OUTROS':
            f_municipio = True
        # Verifica código 316860 (tratando string/float)
        elif '316860' in raw_mun:
            f_municipio = True
        # Verifica texto
        elif ('teofilo' in hist and 'otoni' in hist):
            f_municipio = True

        f_env_cad = (nome_original is not None and str(
            nome_original).strip() != "" and str(nome_original).lower() != "nan")

        # --- LÓGICA DO NOME ---
        f_nome_hist = False
        if f_env_cad and len(nome_full_norm) > 2:
            # Limpa preposições
            nome_limpo = re.sub(r'\b(de|da|do|dos|e)\b',
                                '', nome_full_norm).strip()
            nome_limpo = re.sub(r'\s+', ' ', nome_limpo)
            partes = nome_limpo.split()
            primeiro = partes[0]
            ultimo = partes[-1] if len(partes) > 1 else ""

            if nome_limpo in hist:
                f_nome_hist = True
            elif f_func and (primeiro in hist) and len(primeiro) > 2:
                f_nome_hist = True
            elif (primeiro in hist) and (ultimo and ultimo in hist):
                f_nome_hist = True

        # --- CONSOLIDAÇÃO ---
        registro_valido = all(
            [f_municipio, f_env_cad, f_nome_hist, f_unle, f_unidade, f_func, f_end])

        motivos = []
        if not f_municipio:
            motivos.append("Município incorreto")
        if not f_env_cad:
            motivos.append("Sem Envolvido Qualificado")
        if not f_unle:
            motivos.append('Faltando "UNLE"')
        if not f_unidade:
            motivos.append("Faltando Unidade COPASA")
        if not f_func:
            motivos.append('Faltando menção a "Funcionário/Vigilante"')
        if not f_nome_hist:
            motivos.append("Nome do Envolvido não encontrado no Histórico")
        if not f_end:
            motivos.append("Faltando Endereço")

        # Recupera BOS
        bos = row.get('bos') or row.get('numero_ocorrencia') or 'Desconhecido'

        resultados.append({
            "bos": bos,
            "hora_fato": row.get('data_hora_fato'),
            "codigo_logradouro": row.get(col_logradouro),
            "local_fato": local_identificado,
            "ano": 0,  # Será ajustado abaixo
            "mês": 0,  # Será ajustado abaixo
            "histórico_ocorrência": row.get('historico_ocorrencia', ''),
            "flag_município": f_municipio,
            "envolvido_cadastrado": f_env_cad,
            "unle_no_historico": f_unle,
            "unidade_no_histórico": f_unidade,
            "nome_do_envolvido_no_historico": f_nome_hist,
            "endereço_no_histórico": f_end,
            "registro_válido": registro_valido,
            "motivos_caso_inválido": ", ".join(motivos) if motivos else None
        })

    df_res = pd.DataFrame(resultados)

    # Ajuste final de datas
    if not df_res.empty and 'hora_fato' in df_res.columns:
        df_res['hora_fato'] = pd.to_datetime(
            df_res['hora_fato'], errors='coerce')
        df_res['ano'] = df_res['hora_fato'].dt.year.fillna(2025).astype(int)
        df_res['mês'] = df_res['hora_fato'].dt.month.fillna(12).astype(int)

    return df_res


def aplicar_auditoria_manual(df):
    """Sobrescreve a lógica automática com as marcações manuais salvas no JSON."""
    if not os.path.exists(AUDIT_FILE_PATH):
        return df

    try:
        with open(AUDIT_FILE_PATH, 'r') as f:
            auditoria = json.load(f)

        # Garante que a coluna bos é string para comparação
        df['bos'] = df['bos'].astype(str)

        for bos_key, status in auditoria.items():
            mask = df['bos'] == str(bos_key)
            if any(mask):
                if status == 'validado_manual':
                    df.loc[mask, 'registro_válido'] = True
                    df.loc[mask, 'motivos_caso_inválido'] = "Auditado: Válido Manualmente"
                elif status == 'invalidado_manual':
                    df.loc[mask, 'registro_válido'] = False
                    df.loc[mask, 'motivos_caso_inválido'] = "Auditado: Inválido Manualmente"
        return df
    except Exception as e:
        print(f"[ERRO AUDITORIA] Falha ao aplicar: {e}")
        return df
# =========================
# CACHE E DATA FETCHING
# =========================


def get_dataframe_from_cache_or_bisp():
    cache_info = {"source": "desconhecida", "last_updated": None}

    # 1. Checa data da última atualização no Banco
    with open(SQL_ATUALIZACAO_PATH, 'r', encoding='utf-8') as f:
        sql_check_query = f.read()

    last_bisp_update_df = fetch_data_from_bisp(
        sql_check_query, DB_USER, DB_PASSWORD)
    if last_bisp_update_df is None or last_bisp_update_df.empty:
        raise Exception("Falha ao obter data de atualização no BISP.")

    last_bisp_update = pd.to_datetime(
        last_bisp_update_df.iloc[0, 0]).tz_localize('UTC')

    # 2. Checa status do Cache Local
    last_cache_update = None
    if os.path.exists(CACHE_STATUS_PATH):
        try:
            with open(CACHE_STATUS_PATH, 'r') as f:
                cache_status = json.load(f)
                last_cache_update = datetime.fromisoformat(
                    cache_status['last_updated']).replace(tzinfo=timezone.utc)
        except:
            last_cache_update = None

    # Verifica existência física
    csv_exists = os.path.exists(CACHE_FILE_PATH)

    # 3. Decisão: Atualizar ou Usar Cache?
    # Se você apagar o arquivo CSV, ele cairá aqui
    if not last_cache_update or last_bisp_update > last_cache_update or not csv_exists:
        print("   - [CACHE] Atualizando cache a partir do BISP...")

        with open(SQL_DIAGNOSTICO_PATH, 'r', encoding='utf-8') as f:
            sql_diagnostico_query = f.read()

        # A: Busca dados BRUTOS do Bisp
        df_raw = fetch_data_from_bisp(
            sql_diagnostico_query, DB_USER, DB_PASSWORD)

        if df_raw is None:
            raise Exception("Falha ao buscar dados de diagnóstico do BISP.")

        # === CORREÇÃO CRUCIAL: Padronizar colunas para minúsculo ===
        df_raw.columns = [c.strip().lower() for c in df_raw.columns]

        # B: Processa via Python
        df = processar_dados_python(df_raw)

        # C: Salva o CSV já processado
        df.to_csv(CACHE_FILE_PATH, index=False)

        now_utc = datetime.now(timezone.utc)
        with open(CACHE_STATUS_PATH, 'w') as f:
            json.dump({'last_updated': now_utc.isoformat()}, f)

        cache_info["source"] = "BISP (Processado e Atualizado Agora)"
        cache_info["last_updated"] = now_utc.isoformat()
    else:
        print("   - [CACHE] Lendo dados do arquivo local.")
        df = pd.read_csv(CACHE_FILE_PATH)
        cache_info["source"] = "Cache Local"
        cache_info["last_updated"] = last_cache_update.isoformat()

    # Normalização final de colunas para o Front-End
    df.columns = [c.strip().lower().replace(' ', '_') for c in df.columns]

    # Converte strings de booleanos de volta para True/False real
    if 'registro_válido' in df.columns:
        df['registro_válido'] = df['registro_válido'].astype(
            str).str.lower().isin(['true', '1', 't'])

    df = aplicar_auditoria_manual(df)

    return df, cache_info


@app.route('/api/dados-diagnostico')
def get_dados_diagnostico():
    try:
        df, cache_info = get_dataframe_from_cache_or_bisp()
        coluna_local = "local_fato"
        coluna_valido = "registro_válido"
        coluna_motivo = "motivos_caso_inválido"

        metas = {
            'BARRAGEM TODOS OS SANTOS': 5,
            'CEAM - BARRAGEM TODOS OS SANTOS': 5,
            'AGÊNCIA DE ATENDIMENTO': 5,
            'ALMOXARIFADO SÃO JOSÉ': 5
        }

        if coluna_local not in df.columns or coluna_valido not in df.columns:
            raise KeyError("Coluna essencial ausente.")

        # --------- Filtros vindos da querystring ---------
        filtro_local_val = request.args.get('filtro_local', '')
        filtro_motivo_val = request.args.get('filtro_motivo', '').lower()
        filtro_cia = request.args.get('filtro_cia', '')
        filtro_valido = request.args.get('filtro_valido', '')
        sort_by = request.args.get('sort_by', '').lower().replace(' ', '_')
        sort_order = request.args.get('sort_order', 'asc')
        filtro_mes = request.args.get('filtro_mes', '')  # <<-- pegar cedo

        # Começa do df completo e vai aplicando os filtros
        df_filtrado = df.copy()

        # Filtro por mês/ano (ex.: "11-2025")
        if filtro_mes:
            mes, ano = filtro_mes.split('-')
            df_filtrado = df_filtrado[
                (df_filtrado['mês'] == int(mes)) & (
                    df_filtrado['ano'] == int(ano))
            ]

        # Filtro por CIA
        locais_cia = {
            "42": ["BARRAGEM TODOS OS SANTOS", "CEAM - BARRAGEM TODOS OS SANTOS"],
            "47": ["AGÊNCIA DE ATENDIMENTO", "ALMOXARIFADO SÃO JOSÉ"]
        }
        if filtro_cia and filtro_cia in locais_cia:
            df_filtrado = df_filtrado[df_filtrado[coluna_local].isin(
                locais_cia[filtro_cia])]

        # Filtro por local
        if filtro_local_val:
            df_filtrado = df_filtrado[df_filtrado[coluna_local]
                                      == filtro_local_val]

        # Filtro válidos/ inválidos
        if filtro_valido == 'validos':
            df_filtrado = df_filtrado[df_filtrado[coluna_valido] == True]
        elif filtro_valido == 'invalidos':
            df_filtrado = df_filtrado[df_filtrado[coluna_valido] == False]

        # Filtro por motivo (texto livre)
        if filtro_motivo_val and coluna_motivo in df_filtrado.columns:
            df_filtrado = df_filtrado[
                df_filtrado[coluna_motivo].astype(str).str.lower(
                ).str.contains(filtro_motivo_val, na=False)
            ]

        # Ordenação
        if sort_by and sort_by in df_filtrado.columns:
            df_filtrado = df_filtrado.sort_values(
                by=sort_by, ascending=(sort_order == 'asc'), na_position='last')

        # --------- AGORA sim: calcula as estatísticas usando o df_filtrado ---------
        stats_df = df_filtrado.groupby(coluna_local)[coluna_valido].agg(
            VÁLIDOS=lambda x: x.sum(),
            INVÁLIDOS=lambda x: (~x).sum()
        ).reset_index()
        stats_df['META'] = stats_df[coluna_local].map(metas)
        stats_df['% CUMPRIMENTO'] = (
            stats_df['VÁLIDOS'] / stats_df['META'] * 100).fillna(0).round(2)
        stats_df['RESTANTES'] = stats_df['META'] - stats_df['VÁLIDOS']
        stats_df = stats_df.rename(columns={coluna_local: 'ENDEREÇO'})
        stats_json = stats_df[['ENDEREÇO', 'META', 'VÁLIDOS', 'INVÁLIDOS', '% CUMPRIMENTO', 'RESTANTES']]\
            .to_dict(orient='records')

        # --------- Tabela detalhada (já está com todos os filtros aplicados) ---------
        colunas_para_exibir = ['bos', 'hora_fato', 'local_fato',
                               'histórico_ocorrência', 'motivos_caso_inválido', 'registro_válido']
        colunas_para_exibir = [
            c for c in colunas_para_exibir if c in df_filtrado.columns]
        df_final = df_filtrado[colunas_para_exibir]
        df_final.columns = [c.replace('_', ' ').upper()
                            for c in df_final.columns]
        df_final = df_final.replace({np.nan: None})

        return jsonify({
            "stats": stats_json,
            "details": df_final.to_dict(orient='records'),
            "cache_info": cache_info
        })
    except Exception as e:
        print(f"[ERRO DIAGNÓSTICO] {e}")
        return jsonify({"error": str(e)}), 500


# =========================
# COMPRESSOR (Ghostscript)
# =========================
BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = BASE_DIR / "uploads_pdf"
OUTPUT_DIR = BASE_DIR / "outputs_pdf"
for p in (UPLOAD_DIR, OUTPUT_DIR):
    p.mkdir(parents=True, exist_ok=True)


def _gs_bin():
    env = os.getenv("GS_BIN")
    if env:
        return env
    for name in ["gswin64c", "gswin32c", "gs"]:
        if shutil.which(name):
            return name
    return None


def compress_pdf_gs(input_pdf: Path, output_pdf: Path, quality: str = "ebook"):
    gs = _gs_bin()
    if not gs:
        raise RuntimeError(
            "Ghostscript não encontrado. Instale ou configure GS_BIN.")

    # Configurações base
    base_cmd = [
        gs, "-q", "-dNOPAUSE", "-dBATCH", "-dSAFER",
        "-sDEVICE=pdfwrite",
        "-dCompatibilityLevel=1.4",
        "-dPDFSETTINGS=/ebook",  # fallback
        f"-sOutputFile={str(output_pdf)}", str(input_pdf)
    ]

    # === AJUSTES PERSONALIZADOS POR NÍVEL ===
    extra_args = []

    if quality == "screen":
        extra_args = [
            "-dColorImageDownsampleType=/Bicubic",
            "-dColorImageResolution=72",
            "-dGrayImageDownsampleType=/Bicubic",
            "-dGrayImageResolution=72",
            "-dMonoImageDownsampleType=/Bicubic",
            "-dMonoImageResolution=72",
            "-dDownsampleColorImages=true",
            "-dDownsampleGrayImages=true",
            "-dDownsampleMonoImages=true",
            "-dOptimize=true",
            "-dEmbedAllFonts=false",
            "-dSubsetFonts=true",
            "-dConvertImagesToIndexed=true",
            "-dAutoFilterColorImages=true",
            "-dAutoFilterGrayImages=true",
            "-dColorImageFilter=/FlateEncode",
            "-dGrayImageFilter=/FlateEncode"
        ]

    elif quality == "ebook":
        extra_args = [
            "-dColorImageResolution=150",
            "-dGrayImageResolution=150",
            "-dMonoImageResolution=150",
            "-dDownsampleColorImages=true",
            "-dDownsampleGrayImages=true",
            "-dDownsampleMonoImages=true",
            "-dColorImageDownsampleType=/Bicubic",
            "-dGrayImageDownsampleType=/Bicubic",
            "-dMonoImageDownsampleType=/Bicubic"
        ]

    elif quality == "printer":
        extra_args = [
            "-dColorImageResolution=300",
            "-dGrayImageResolution=300",
            "-dMonoImageResolution=300"
        ]

    # Aplica
    cmd = base_cmd[:-2] + extra_args + base_cmd[-2:]
    print(f"[GS] Executando: {' '.join(cmd[:5])} ... {len(cmd)} args")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"[GS ERRO] {result.stderr}")
        raise RuntimeError(f"Ghostscript falhou: {result.stderr[:200]}")

    if not output_pdf.exists() or output_pdf.stat().st_size == 0:
        raise RuntimeError("Arquivo de saída vazio ou não gerado.")


@app.route("/api/compress", methods=["POST"])
def api_compress():
    quality = (request.form.get("quality") or "ebook").lower().strip()
    if quality not in {"screen", "ebook", "printer"}:
        return jsonify({"error": "quality inválido. Use screen|ebook|printer"}), 400

    if "file" in request.files and request.files["file"].filename:
        pdf = request.files["file"]
        fname = secure_filename(pdf.filename)
        if not fname.lower().endswith(".pdf"):
            return jsonify({"error": "Envie um arquivo .pdf"}), 400
        in_path = UPLOAD_DIR / fname
        out_name = in_path.stem + f"__{quality}.pdf"
        out_path = OUTPUT_DIR / out_name
        pdf.save(in_path)
        try:
            compress_pdf_gs(in_path, out_path, quality=quality)
        except Exception as e:
            return jsonify({"error": str(e)}), 500
        return send_file(out_path, as_attachment=True, download_name=out_name)

    folder = (request.form.get("folder_path") or "").strip()
    if folder:
        folder_path = Path(folder)
        if not folder_path.exists() or not folder_path.is_dir():
            return jsonify({"error": "Pasta não encontrada/sem acesso."}), 400

        for f in OUTPUT_DIR.glob("*"):
            if f.is_file():
                f.unlink()

        pdfs = list(folder_path.glob("*.pdf"))
        if not pdfs:
            return jsonify({"error": "Nenhum PDF encontrado na pasta."}), 400

        for pdf_in in pdfs:
            try:
                out = OUTPUT_DIR / (pdf_in.stem + f"__{quality}.pdf")
                compress_pdf_gs(pdf_in, out, quality=quality)
            except Exception as e:
                print(f"[Compress] Falha em {pdf_in}: {e}")

        zip_base = OUTPUT_DIR / f"pdfs_compactados_{quality}"
        zip_file = shutil.make_archive(str(zip_base), "zip", OUTPUT_DIR)
        return send_file(zip_file, as_attachment=True, download_name=Path(zip_file).name)

    return jsonify({"error": "Envie um arquivo ou informe uma pasta."}), 400

# =========================
# MONTAGEM (seleção de páginas)
# =========================


def _parse_pages_spec(spec: str, total_pages: int) -> list[int]:
    pages = []
    spec = (spec or "").replace(" ", "")
    if not spec:
        return pages
    for part in spec.split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            a, b = int(a), int(b)
            rng = range(a, b + 1) if a <= b else range(a, b - 1, -1)
            for p in rng:
                if 1 <= p <= total_pages:
                    pages.append(p - 1)
        else:
            p = int(part)
            if 1 <= p <= total_pages:
                pages.append(p - 1)
    return pages


def _merge_selected(files_and_specs: list[tuple[Path, str]]) -> bytes:
    writer = PdfWriter()
    for pdf_path, spec in files_and_specs:
        reader = PdfReader(str(pdf_path))
        idxs = _parse_pages_spec(spec, len(reader.pages))
        for i in idxs:
            writer.add_page(reader.pages[i])
    buf = io.BytesIO()
    writer.write(buf)
    out = buf.getvalue()
    buf.close()
    return out


@app.route("/api/extract-merge", methods=["POST"])
def api_extract_merge():
    pages_spec = (request.form.get("pages") or "").strip()
    if not pages_spec:
        return jsonify({"error": "Informe 'pages'. Ex.: 1,4-6,9,12|3,5"}), 400

    parts_specs = [p.strip() for p in pages_spec.split("|") if p.strip()]
    files_and_specs = []

    if "files" not in request.files:
        return jsonify({"error": "Envie 'files' (upload dos PDFs)."}), 400

    files = request.files.getlist("files")
    if len(files) != len(parts_specs):
        return jsonify({"error": f"Quantidade de arquivos ({len(files)}) difere das partes em 'pages' ({len(parts_specs)})."}), 400

    temp_paths = []
    try:
        for i, up in enumerate(files):
            fname = secure_filename(up.filename or f"arquivo_{i}.pdf")
            if not fname.lower().endswith(".pdf"):
                return jsonify({"error": f"Arquivo inválido: {fname} (apenas .pdf)"}), 400

            p = UPLOAD_DIR / fname
            up.save(p)
            temp_paths.append(p)

            # Testar leitura do PDF
            try:
                reader = PdfReader(str(p))
                if reader.is_encrypted:
                    return jsonify({"error": f"O PDF '{fname}' está protegido por senha. Remova a proteção antes."}), 400
            except Exception as e:
                return jsonify({"error": f"PDF corrompido ou inválido: {fname} → {str(e)}"}), 400

        for pth, spec in zip(temp_paths, parts_specs):
            files_and_specs.append((pth, spec))

        try:
            merged = _merge_selected(files_and_specs)
        except Exception as e:
            return jsonify({"error": f"Falha ao montar PDF: {str(e)}"}), 500

        out_name = (request.form.get("out_name") or "pdf_montado.pdf").strip()
        if not out_name.lower().endswith(".pdf"):
            out_name += ".pdf"

        return send_file(
            io.BytesIO(merged),
            as_attachment=True,
            download_name=out_name,
            mimetype="application/pdf"
        )

    finally:
        # Limpeza segura
        for p in temp_paths:
            try:
                if p.exists():
                    p.unlink()
            except:
                pass

# =========================
# PÁGINAS
# =========================


@app.route("/pdf")
def pdf_ui():
    return render_template("pdf_tool.html")


@app.route('/api/copasa/auditar', methods=['POST'])
def api_auditar_copasa():
    try:
        data = request.json
        bos = str(data.get('bos'))
        status = data.get('status')  # 'validado_manual' ou 'invalidado_manual'

        auditoria = {}
        if os.path.exists(AUDIT_FILE_PATH):
            with open(AUDIT_FILE_PATH, 'r') as f:
                auditoria = json.load(f)

        auditoria[bos] = status

        with open(AUDIT_FILE_PATH, 'w') as f:
            json.dump(auditoria, f, indent=4)

        return jsonify({"status": "success", "message": f"Registro {bos} atualizado."})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == '__main__':
    print("Servidor de automação iniciado.")
    print("Atualização da planilha: http://10.14.56.162:8088/")
    print("Painel de diagnóstico:  http://10.14.56.162:8088/diagnostico")
    app.run(host='0.0.0.0', port=8088, debug=False)
