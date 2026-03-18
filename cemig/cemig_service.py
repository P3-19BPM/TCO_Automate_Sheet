import pyodbc
import polars as pl
import os
import threading
from datetime import datetime
from .cemig_sql import get_sql_cemig
from .cemig_repository import carregar_cache, salvar_cache
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

# ============================
# STATUS GLOBAL (LiveLog)
# ============================
cemig_log = {
    "running": False,
    "passos": [],
    "ultimo_erro": None,
    "timestamp": None,
}
_log_lock = threading.Lock()

def _log(msg, erro=False):
    ts = datetime.now().strftime('%H:%M:%S')
    linha = f"[{ts}] {'[ERRO] ' if erro else ''}{msg}"
    print(linha)
    with _log_lock:
        cemig_log["passos"].append(linha)
        if erro:
            cemig_log["ultimo_erro"] = linha

def iniciar_log():
    with _log_lock:
        cemig_log["running"] = True
        cemig_log["passos"] = []
        cemig_log["ultimo_erro"] = None
        cemig_log["timestamp"] = datetime.now().isoformat()

def finalizar_log():
    with _log_lock:
        cemig_log["running"] = False

def get_status_log():
    with _log_lock:
        return dict(cemig_log)

# ============================
# CONEXÃO BISP
# ============================
def fetch_data_from_bisp(sql_query, user, pwd):
    _log("Iniciando conexão com o Data Warehouse BISP...")
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
            _log("Conexão estabelecida! Executando SQL com RegEx RLIKE...")
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [column[0] for column in cursor.description]
            _log("SQL executado. Aguardando retorno dos dados...")
            records = cursor.fetchall()
            data = [tuple(row) for row in records]

            if not data:
                _log("AVISO: Nenhum registro encontrado para os critérios da consulta.")
                return pl.DataFrame()

            _log(f"{len(data)} registros recebidos. Convertendo para Polars DataFrame...")
            df = pl.DataFrame(data, schema=columns, orient="row")
            _log("Conversão concluída com sucesso.")
            return df
    except Exception as e:
        _log(f"Falha crítica na extração: {str(e)}", erro=True)
        return None


# ============================
# ORQUESTRADOR
# ============================
def buscar_dados_cemig(ano=None, force_refresh=False):
    iniciar_log()
    if ano is None:
        ano = datetime.now().year

    _log(f"Requisição recebida — Ano: {ano} | ForceRefresh: {force_refresh}")

    if not force_refresh:
        _log("Verificando cache local (Parquet)...")
        df = carregar_cache()
        if df is not None:
            cache_date = os.path.getmtime('cache/cemig_cache.parquet')
            date_str = datetime.fromtimestamp(cache_date).strftime('%Y-%m-%d %H:%M:%S')
            _log(f"Cache válido encontrado! Dados carregados ({len(df)} linhas). Atualizado em {date_str}.")
            finalizar_log()
            return df, {"source": "Cache Local Parquet", "last_updated": date_str}
        _log("Cache não encontrado ou expirado. Buscando no BISP...")
    else:
        _log("Atualização forçada pelo usuário. Ignorando cache e buscando no BISP...")

    sql = get_sql_cemig().replace(':ANO', str(ano))
    df_raw = fetch_data_from_bisp(sql, DB_USER, DB_PASSWORD)

    if df_raw is not None and not df_raw.is_empty():
        _log("Normalizando nomes de colunas...")
        df_raw = df_raw.rename({c: c.lower().replace(' ', '_') for c in df_raw.columns})
        _log("Salvando novo cache em Parquet...")
        salvar_cache(df_raw)

        cache_date = os.path.getmtime('cache/cemig_cache.parquet')
        date_str = datetime.fromtimestamp(cache_date).strftime('%Y-%m-%d %H:%M:%S')
        _log("✔ Atualização concluída com sucesso! Cache atualizado.")
        finalizar_log()
        return df_raw, {"source": "BISP (Atualizado Agora)", "last_updated": date_str}

    _log("Falha ao obter dados. Verifique o log de erros.", erro=True)
    finalizar_log()
    return pl.DataFrame(), {"source": "Sem Dados", "last_updated": "Nunca"}
