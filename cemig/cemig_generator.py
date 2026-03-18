import polars as pl

def gerar_estatisticas(df: pl.DataFrame):
    if df.is_empty():
        return [], []
        
    # Agrupa por semana e município
    agrupado = df.group_by(['semana', 'nome_municipio']).agg(
        pl.len().alias('VÁLIDOS')
    )
    
    # Adicionando META base = 5 por semana/municipio para CEMIG
    # (pode ser ajustado conforme modelo real CEMIG)
    agrupado = agrupado.with_columns([
        pl.lit(5).alias('META'),
        ((pl.col('VÁLIDOS') / pl.lit(5)) * 100).round(2).alias('% CUMPRIMENTO'),
        (pl.lit(5) - pl.col('VÁLIDOS')).alias('RESTANTES')
    ]).sort(['semana', 'nome_municipio'])
    
    # Renomeando as colunas para o frontend (mantendo aderente ao existente)
    agrupado = agrupado.rename({
        'nome_municipio': 'ENDEREÇO'
    })
    
    # Evitar problemas com conversão de timedelta/datetime com JSON
    st = agrupado.to_dicts()
    
    detalhes_df = df.sort('data_hora_fato', descending=True)
    dt = detalhes_df.to_dicts()
    
    # Limpa possíveis objetos de datetime python brutos para strings para serializar no json jsonify
    for row in dt:
        for k, v in row.items():
            if type(v).__name__ in ['datetime', 'date', 'Timestamp']:
                row[k] = str(v)
                
    return st, dt
