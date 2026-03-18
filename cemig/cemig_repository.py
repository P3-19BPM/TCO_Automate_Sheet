import polars as pl
import os
import time

CACHE_FILE = 'cache/cemig_cache.parquet'

def carregar_cache(ttl_hours=12):
    if os.path.exists(CACHE_FILE):
        # Verifica se o cache expirou
        mod_time = os.path.getmtime(CACHE_FILE)
        age_hours = (time.time() - mod_time) / 3600
        if age_hours > ttl_hours:
            return None # Retorna None para forçar a atualização
            
        return pl.read_parquet(CACHE_FILE)
    return None

def salvar_cache(df: pl.DataFrame):
    os.makedirs('cache', exist_ok=True)
    df.write_parquet(CACHE_FILE)
