WITH registros_validos AS (
    SELECT
        CASE 
            WHEN OCO.logradouro_codigo = '3361' THEN '03 - AGÊNCIA DE ATENDIMENTO'
            WHEN OCO.logradouro_codigo = '2601' THEN '04 - ALMOXARIFADO SÃO JOSÉ'
            WHEN OCO.logradouro_codigo IN ('-498661','-515062','-497640','-558382') THEN '02 - CEAM - BARRAGEM TODOS OS SANTOS'
            WHEN OCO.logradouro_codigo IN ('-570056','-497579') THEN '01 - BARRAGEM TODOS OS SANTOS'
            ELSE 'OUTROS'
        END AS local_fato_registro,
        YEAR(OCO.data_hora_fato) AS ano,
        MONTH(OCO.data_hora_fato) AS mes,
        OCO.numero_ocorrencia AS numero_bos
    FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
    WHERE
        OCO.natureza_codigo = 'A21000'
        AND OCO.codigo_municipio = 316860
        AND OCO.data_hora_fato >= '2025-10-01'
        AND OCO.logradouro_codigo IN ('3361', '2601', '-498661', '-515062', '-497640', '-558382', '-570056', '-497579')
        AND LOWER(OCO.historico_ocorrencia) LIKE '%copasa%'
),
agrupado AS (
    SELECT
        local_fato_registro,
        mes,
        COUNT(DISTINCT numero_bos) AS quantidade_registros,
        GROUP_CONCAT(DISTINCT CAST(numero_bos AS STRING), '; ') AS "Nº BOS/RAT"
    FROM registros_validos
    GROUP BY local_fato_registro, mes
)
SELECT *
FROM agrupado
ORDER BY local_fato_registro, mes;
