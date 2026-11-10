WITH registros_validos AS (
    SELECT
        CASE 
            WHEN OCO.logradouro_codigo IN ('3361','474640') OR UPPER(COALESCE(OCO.logradouro_nome, '')) RLIKE 'PADRE VIRG[OU]LINO' THEN '03 - AGÊNCIA DE ATENDIMENTO'
            WHEN OCO.logradouro_codigo IN ('2601','471568') OR UPPER(COALESCE(OCO.logradouro_nome, '')) RLIKE 'MARCELO GUEDES' THEN '04 - ALMOXARIFADO SÃO JOSÉ'
            WHEN OCO.logradouro_codigo IN ('-498661','-515062','-497640','-558382','TFL0221','TFL0293','TFL0997') OR UPPER(COALESCE(OCO.logradouro_nome, '')) RLIKE 'TFL ?(0221|0293|0997)' THEN '02 - CEAM - BARRAGEM TODOS OS SANTOS'
            WHEN OCO.logradouro_codigo IN ('-570056','-497579','TFL0282') OR UPPER(COALESCE(OCO.logradouro_nome, '')) RLIKE 'TFL ?0282' THEN '01 - BARRAGEM TODOS OS SANTOS'
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
        -- Codigo OU nome do logradouro: o BISP recodificou os logradouros em 22/10/2026,
        -- o nome continua estavel e evita perder registros numa proxima recodificacao.
        AND (
            OCO.logradouro_codigo IN ('3361', '2601', '-498661', '-515062', '-497640', '-558382', '-570056', '-497579', '474640', '471568', 'TFL0221', 'TFL0293', 'TFL0997', 'TFL0282')
            OR UPPER(COALESCE(OCO.logradouro_nome, '')) RLIKE '(PADRE VIRG[OU]LINO|MARCELO GUEDES|TFL ?0221|TFL ?0282|TFL ?0293|TFL ?0997)'
        )
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
