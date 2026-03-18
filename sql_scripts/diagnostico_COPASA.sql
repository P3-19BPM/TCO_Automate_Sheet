SELECT
    OCO.numero_ocorrencia AS bos,
    OCO.data_hora_fato,
    OCO.logradouro_codigo,
    OCO.historico_ocorrencia,
    OCO.codigo_municipio,
    OCO.nome_municipio,
    E.nome_completo_envolvido,
    E.numero_cpf_cnpj,
    E.tipo_documento_codigo,
    E.numero_documento_id
FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
LEFT JOIN db_bisp_reds_reporting.tb_envolvido_ocorrencia AS E
    ON OCO.numero_ocorrencia = E.numero_ocorrencia
WHERE
    OCO.natureza_codigo = 'A21000'
    AND OCO.codigo_municipio = 316860 
    AND OCO.data_hora_fato >= '2025-10-01'
    AND OCO.data_hora_fato < days_add(last_day(now()), 1)
    AND OCO.logradouro_codigo IN ('3361', '2601', '-498661', '-515062', '-497640', '-558382', '-570056', '-497579')
    AND LOWER(OCO.historico_ocorrencia) LIKE '%copasa%'
    AND (
        E.numero_cpf_cnpj IS NOT NULL
        OR (
            E.tipo_documento_codigo IN ('0801', '0802', '0803', '0809')
            AND E.numero_documento_id IS NOT NULL
        )
    )