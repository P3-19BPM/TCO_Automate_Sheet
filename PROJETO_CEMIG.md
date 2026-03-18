## PROMPT PARA AGENT — MÓDULO CEMIG ISOLADO + SQL BISP (REGEX)

Você atuará como um engenheiro de software sênior especializado em:

* Python (Flask, Polars)
* SQL (Impala / BISP)
* Arquitetura modular
* Sistemas já em produção (NÃO quebrar funcionalidades existentes)

---

# OBJETIVO PRINCIPAL

Criar um módulo **TOTALMENTE ISOLADO chamado CEMIG**, sem alterar:

* rotas existentes
* lógica COPASA
* cache atual
* processamento existente

A nova funcionalidade deve coexistir com o sistema atual.

---

# REGRA CRÍTICA (MANDATÓRIA)

⚠️ NÃO MODIFICAR:

* funções existentes
* rotas já implementadas
* SQL da COPASA
* cache atual
* endpoints atuais

✔ Tudo da CEMIG deve ser criado separado

---

# 1. ESTRUTURA DO MÓDULO CEMIG

Criar nova pasta:

```id="y4v2r3"
cemig/
    __init__.py
    cemig_sql.py
    cemig_service.py
    cemig_repository.py
    cemig_generator.py
```

---

# 2. NOVO ENDPOINT

Adicionar no servidor:

```python id="b8t1vz"
@app.route('/cemig')
def cemig():
```

IMPORTANTE:

* NÃO reutilizar funções da COPASA
* NÃO alterar `/copasa`
* fluxo independente

---

# 3. SQL — AJUSTE PARA CEMIG (REGEX NO HISTÓRICO)

Substituir lógica baseada em `logradouro_codigo`.

Usar **RLIKE (Impala)** para identificar CEMIG no histórico.

---

## NOVO SQL CEMIG

```sql id="j8qv2p"
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
    OCO.historico_ocorrencia

FROM db_bisp_reds_reporting.tb_ocorrencia AS OCO

WHERE 1=1
    AND OCO.natureza_codigo = 'A21000'
    AND YEAR(OCO.data_hora_fato) = :ANO

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
;
```

---

# 4. MELHORIA (IMPORTANTE)

Criar campo derivado:

```sql id="0uvc8y"
CASE
    WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'transformador' THEN 'TRANSFORMADOR'
    WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'poste' THEN 'POSTE'
    WHEN LOWER(OCO.historico_ocorrencia) RLIKE 'rede' THEN 'REDE ELETRICA'
    ELSE 'OUTROS'
END AS TIPO_EVENTO
```

---

# 5. cemig_sql.py

Criar função:

```python id="x2p0sc"
def get_sql_cemig():
    return """SQL ACIMA"""
```

---

# 6. cemig_service.py

Responsável por:

* executar SQL
* retornar dataframe (Polars)

```python id="y9prg5"
def buscar_dados_cemig():
```

Regras:

* usar pyodbc já existente
* converter resultado para Polars
* NÃO usar pandas

---

# 7. cemig_repository.py

Salvar cache separado:

```id="4ztntk"
cache/cemig_cache.parquet
```

Funções:

```python id="91cf6r"
def salvar_cache(df)
def carregar_cache()
```

---

# 8. cemig_generator.py

Responsável por gerar tabela de visitas (igual você já criou).

Usar Polars.

---

# 9. TEMPLATE

Criar:

```id="n0vdfq"
templates/cemig.html
```

Com:

* gráfico ECharts
* tema escuro
* título: "CEMIG - Monitoramento Operacional"

---

# 10. LÓGICA DO ENDPOINT

Fluxo:

```id="q2p4x0"
1. verifica cache
2. se não existir → executa SQL
3. salva parquet
4. processa dados (Polars)
5. envia para template
```

---

# 11. PERFORMANCE

* evitar pandas
* usar Polars
* evitar loops
* usar operações vetorizadas

---

# 12. DIFERENCIAL OPERACIONAL (IMPORTANTE)

Preparar código para:

* classificação por tipo_evento
* análise por semana
* análise por município
* futura integração com GeoJSON

---

# 13. RESULTADO FINAL

Sistema deve ter:

```id="d2b7ep"
/copasa  → continua igual
/cemig   → novo módulo isolado
```

---

# 14. VALIDAÇÃO FINAL

Antes de finalizar:

✔ COPASA continua funcionando
✔ SQL antigo intacto
✔ novo módulo independente
✔ código organizado

---

# INSTRUÇÃO FINAL

Implemente o módulo completo garantindo:

* isolamento total
* performance com Polars
* SQL baseado em REGEX
* compatibilidade com sistema atual
