SELECT 
                OCO.numero_ocorrencia, -- Número da ocorrência 
                OCO.data_hora_fato ,
                OCO.nome_municipio ,
                OCO.natureza_codigo ,
                OCO.natureza_descricao ,
                CASE 
                        WHEN OCO.ind_tco = 'S' THEN 'TCO'
                        WHEN OCO.natureza_codigo = 'I99000' THEN 'USO E CONSUMO'
                END ind_tco,
                OCO.relator_matricula ,
                OCO.relator_nome,
                OCO.data_hora_fechamento
    FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO
        WHERE 1=1
                --AND ENV.id_envolvimento IN(35,36,44) -- Filtra somente envolvidos classificados como autor, co-autor, suspeito
                --AND ENV.natureza_ocorrencia_codigo IN ('B01121','B02001') -- Filtra pela natureza específica da ocorrência
                --AND ENV.id_tipo_prisao_apreensao IN (1,2,3,4,6,7) -- Filtra os tipos de prisão e apreensão válidos
        --AND ENV.digitador_id_orgao IN (0,1) -- Registro realizado por determinados órgãos (PM,PC)
        --AND OCO.codigo_municipio in (310470,311080,311300,311545,312675,312680,313230,313270,313507,313700,313920,314490,314530,314535,314620,314850,315000,315240,316330,316555,316860)
        AND OCO.codigo_municipio in (316860, 315240, 313700, 310470, 314850, 314535, 314620)
        --AND OCO.digitador_id_orgao IN (0) -- Registro feito por órgãos específicos(PM , PC)
                AND OCO.unidade_responsavel_registro_nome like '%19 BPM%'
                AND (OCO.ind_tco = 'S' OR OCO.natureza_codigo = 'I99000')
                AND OCO.data_hora_fato BETWEEN '2025-01-01 00:00:00.000' AND '2025-12-31 00:00:00.000' -- Período de análise