def get_sql_cemig():
    return """
-- NAME: CONSULTA_CEMIG_REGEX
SELECT
    'CEMIG' AS LOCAL,
    OCO.numero_ocorrencia,
    WEEK(OCO.data_hora_fato) AS SEMANA,
    OCO.data_hora_fato,
    OCO.natureza_codigo,
    OCO.logradouro_codigo,
    OCO.logradouro_nome,
    OCO.nome_municipio,
    OCO.historico_ocorrencia,
    CASE
        WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'transformador' THEN 'TRANSFORMADOR'
        WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'poste' THEN 'POSTE'
        WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'rede' THEN 'REDE ELETRICA'
        ELSE 'OUTROS'
    END AS TIPO_EVENTO
FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
WHERE 1=1
    AND OCO.natureza_codigo = 'A21000'
    AND YEAR(OCO.data_hora_fato) >=2025
    -- IDENTIFICAÇÃO POR TEXTO (CEMIG)
    AND (
        LOWER(OCO.historico_ocorrencia) RLIKE 'cemig'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'energia'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'rede eletrica'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'rede elétrica'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'poste'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'fiação'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'fiacao'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'transformador'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'subestacao'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'subestação'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'alta tensao'
        OR LOWER(OCO.historico_ocorrencia) RLIKE 'alta tensão'
    )
"""
