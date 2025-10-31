WITH
base AS (
SELECT
OCO.*,
LOWER(OCO.historico_ocorrencia) AS h,
TRIM(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(OCO.historico_ocorrencia),'á','a'),'à','a'),'â','a'),'ã','a'),'é','e'),'ê','e'),'í','i'),'ì','i'),'ó','o'),'ô','o'),'õ','o'),'ú','u'),'ü','u'),'ç','c'),'´',''),'Ã','a'),'É','e'),'Á','a'),'Ç','c'),'Õ','o'), ' +', ' ')) AS h_normalizado
FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
WHERE
OCO.natureza_codigo = 'A21000'
AND OCO.codigo_municipio = 316860
AND OCO.data_hora_fato >= '2025-10-01'
AND OCO.data_hora_fato < '2025-11-01'
AND OCO.logradouro_codigo IN ('3361', '2601', '-498661', '-515062', '-497640', '-558382', '-570056', '-497579')
AND LOWER(OCO.historico_ocorrencia) LIKE '%copasa%'
),
envolvidos_identificados AS (
SELECT
E.numero_ocorrencia,
E.nome_completo_envolvido,
TRIM(REGEXP_REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(E.nome_completo_envolvido),'á','a'),'à','a'),'â','a'),'ã','a'),'é','e'),'ê','e'),'í','i'),'ì','i'),'ó','o'),'ô','o'),'õ','o'),'ú','u'),'ü','u'),'ç','c'),'´',''),'Ã','a'),'É','e'),'Á','a'),'Ç','c'),'Õ','o'), ' +', ' ')) AS nome_completo_normalizado
FROM db_bisp_reds_reporting.tb_envolvido_ocorrencia AS E
WHERE
(E.numero_cpf_cnpj IS NOT NULL
OR (
E.tipo_documento_codigo IN ('0801', '0802', '0803', '0809')
AND E.numero_documento_id IS NOT NULL
))
AND E.numero_ocorrencia IN (SELECT numero_ocorrencia FROM base)
),
flags_por_envolvido AS (
SELECT
b.*,
e.nome_completo_envolvido,
e.nome_completo_normalizado,
(e.nome_completo_envolvido IS NOT NULL) AS tem_envolvido_qualificado,
(
e.nome_completo_normalizado IS NOT NULL AND e.nome_completo_normalizado != ''
AND instr(b.h_normalizado, e.nome_completo_normalizado) > 0
) AS flag_nome_no_historico
FROM base AS b
LEFT JOIN envolvidos_identificados AS e ON b.numero_ocorrencia = e.numero_ocorrencia
),
flags_consolidadas AS (
SELECT
f.numero_ocorrencia,
f.data_hora_fato,
f.logradouro_codigo,
f.historico_ocorrencia,
f.codigo_municipio,
f.h,
MAX(CAST(f.tem_envolvido_qualificado AS INT)) = 1 AS flag_envolvido,
MAX(CAST(f.flag_nome_no_historico AS INT)) = 1 AS flag_nome_no_historico,
(f.h LIKE '%unle%' OR f.h LIKE '%unidade de negócio%' OR f.h LIKE '%unidade de negocio%') AS flag_hist_unle,
(f.h LIKE '%copasa%' OR f.h LIKE '%ete%' OR f.h LIKE '%eta%' OR f.h LIKE '%barragem%' OR f.h LIKE '%reservat%' OR f.h LIKE '%elevat%' OR f.h LIKE '%almoxarifado%' OR f.h LIKE '%agência de atendimento%' OR f.h LIKE '%agencia de atendimento%' OR f.h LIKE '%agencia atendimento%') AS flag_hist_unidade,
(f.h LIKE '%funcionário%' OR f.h LIKE '%RGIO SANTOS COELHO%' OR f.h LIKE '%funcio%' OR f.h LIKE '%superviso%' OR f.h LIKE '%colaborador%' OR f.h LIKE '%vigilante%' OR f.h LIKE '%vigilantes%') AS flag_hist_func,
(f.h LIKE '%logradouro%' OR f.h LIKE '%coord%' OR f.h LIKE '%lat%' OR f.h LIKE '%long%' OR f.h LIKE '%bairro%' OR f.h LIKE '%rua%' OR f.h LIKE '%município%' OR f.h LIKE '%municipio%') AS flag_hist_endereco
FROM flags_por_envolvido AS f
GROUP BY f.numero_ocorrencia, f.data_hora_fato, f.logradouro_codigo, f.historico_ocorrencia, f.codigo_municipio, f.h
)
SELECT
f.numero_ocorrencia AS "BOS",
f.data_hora_fato AS "HORA FATO",
f.logradouro_codigo AS "CODIGO LOGRADOURO",
CASE
WHEN f.logradouro_codigo = '3361' THEN 'AGÊNCIA DE ATENDIMENTO'
WHEN f.logradouro_codigo = '2601' THEN 'ALMOXARIFADO SÃO JOSÉ'
WHEN f.logradouro_codigo IN ('-498661','-515062','-497640','-558382') THEN 'CEAM - BARRAGEM TODOS OS SANTOS'
WHEN f.logradouro_codigo IN ('-570056','-497579') THEN 'BARRAGEM TODOS OS SANTOS'
ELSE 'OUTROS'
END AS "LOCAL FATO",
YEAR(f.data_hora_fato) AS "ANO",
MONTH(f.data_hora_fato) AS "MÊS",
f.historico_ocorrencia AS "HISTÓRICO OCORRÊNCIA",
(f.codigo_municipio = 316860) AS "FLAG MUNICÍPIO",
f.flag_envolvido AS "ENVOLVIDO CADASTRADO",
f.flag_hist_unle AS "UNLE NO HISTORICO",
f.flag_hist_unidade AS "UNIDADE NO HISTÓRICO",
f.flag_nome_no_historico AS "NOME DO ENVOLVIDO NO HISTORICO",
f.flag_hist_endereco AS "ENDEREÇO NO HISTÓRICO",
( (f.codigo_municipio = 316860) AND f.flag_envolvido AND f.flag_nome_no_historico AND f.flag_hist_unle AND f.flag_hist_unidade AND f.flag_hist_func AND f.flag_hist_endereco) AS "REGISTRO VÁLIDO",
CONCAT_WS(', ',
CASE WHEN f.codigo_municipio != 316860 THEN 'Município incorreto' END,
CASE WHEN NOT f.flag_envolvido THEN 'Sem Envolvido Qualificado' END,
CASE WHEN NOT f.flag_hist_unle THEN 'Faltando "UNLE"' END,
CASE WHEN NOT f.flag_hist_unidade THEN 'Faltando Unidade COPASA' END,
CASE WHEN NOT f.flag_hist_func THEN 'Faltando menção a "Funcionário"' END,
CASE WHEN NOT f.flag_nome_no_historico THEN 'Nome do Envolvido não encontrado no Histórico' END,
CASE WHEN NOT f.flag_hist_endereco THEN 'Faltando Endereço' END
) AS "MOTIVOS CASO INVÁLIDO"
FROM flags_consolidadas AS f
ORDER BY "LOCAL FATO", "MÊS", "BOS"
