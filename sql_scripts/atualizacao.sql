SELECT MAX(oco.data_hora_inclusao)
FROM db_bisp_reds_reporting.tb_ocorrencia oco
WHERE oco.data_hora_inclusao > DATE_SUB(NOW(), INTERVAL 2 DAYS)