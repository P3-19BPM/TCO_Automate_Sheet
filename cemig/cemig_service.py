import pyodbc
import polars as pl
import os
from datetime import datetime
from .cemig_sql import get_sql_cemig
from .cemig_repository import carregar_cache, salvar_cache
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv('DB_USERNAME')
DB_PASSWORD = os.getenv('DB_PASSWORD')

def fetch_data_from_bisp(sql_query, user, pwd):
    print(f"\n[BISP CEMIG] Executando consulta...")
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
            cursor = conn.cursor()
            cursor.execute(sql_query)
            columns = [column[0] for column in cursor.description]
            records = cursor.fetchall()
            
            data = [tuple(row) for row in records]
            if not data:
                return pl.DataFrame()
            
            df = pl.DataFrame(data, schema=columns, orient="row")
            print(f"   - [BISP CEMIG] Consulta executada. {len(df)} registros encontrados.")
            return df
    except Exception as e:
        print(f"\n[ERRO BISP CEMIG] Falha ao buscar dados: {e}")
        return None

def buscar_dados_cemig(ano=None, force_refresh=False):
    if ano is None:
        ano = datetime.now().year
        
    if not force_refresh:
        df = carregar_cache()
        if df is not None:
            cache_date = os.path.getmtime('cache/cemig_cache.parquet')
            date_str = datetime.fromtimestamp(cache_date).strftime('%Y-%m-%d %H:%M:%S')
            return df, {"source": "Cache Local Parquet", "last_updated": date_str}
        
    sql = get_sql_cemig().replace(':ANO', str(ano))
    df_raw = fetch_data_from_bisp(sql, DB_USER, DB_PASSWORD)
    
    if df_raw is not None and not df_raw.is_empty():
        df_raw = df_raw.rename({c: c.lower().replace(' ', '_') for c in df_raw.columns})
        salvar_cache(df_raw)
        
        cache_date = os.path.getmtime('cache/cemig_cache.parquet')
        date_str = datetime.fromtimestamp(cache_date).strftime('%Y-%m-%d %H:%M:%S')
        return df_raw, {"source": "BISP (Polars e Parquet Agora)", "last_updated": date_str}
    
    return pl.DataFrame(), {"source": "Sem Dados", "last_updated": "Nunca"}
