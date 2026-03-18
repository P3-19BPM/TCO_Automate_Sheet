# Documentação dos Projetos 📚

**Visão Geral**

Este repositório contém ferramentas para automação e diagnóstico de registros de **TCO (Termos Circunstanciados de Ocorrência)**: coleta via BISP, processamento/validação em Python (pandas), painel web de diagnóstico e atualização automática de Google Sheets.

---

## Sumário dos Componentes 🔧

- **`api_server.py`**  — Servidor Flask principal (porta **8088**) com: painel de diagnóstico (`/diagnostico`), endpoints de compressão e manipulação de PDFs (`/api/compress`, `/api/extract-merge`), endpoints de diagnóstico e auditoria (`/api/dados-diagnostico`, `/api/copasa/auditar`, `/api/copasa/export-xlsx`) e rotina para atualizar Google Sheets via conta de serviço.

- **`main.py`** — Versão mais simples/alternativa para atualizar Google Sheets (porta **5000**). Endpoint protegido `/run-update/<SECRET_KEY>` que dispara atualização em background usando `threading`.

- **`copasa.py`** — Pequeno módulo utilitário para gravar/ler auditoria manual em `cache/auditoria_copasa.json` e aplicar sobrescritas no dataframe (funções: `carregar_auditoria`, `salvar_auditoria`, `aplicar_auditoria`).

- **`templates/`** — HTMLs do painel (`diagnostico.html`, `copasa.html`), contém a UI JavaScript para filtros, exportação CSV, ordenação e chamadas aos endpoints API.

- **`sql_scripts/`** — Scripts SQL usados: `registros_tco_2025.sql`, `diagnostico_COPASA.sql`, `atualizacao.sql`.

- **`config/`** — Arquivos sensíveis: `credentials.json` / `credenciais-robo.json` (Google) e `cacerts.pem` (certificado para ODBC/Impala).

- **`cache/`** — Cache local e metadados: `diagnostico_cache.csv`, `cache_status.json`, `auditoria_copasa.json`.

- **`uploads_pdf/`**, **`outputs_pdf/`** — Uploads temporários e resultados de compressão/merge.

- **`iniciar_servidor_oculto.bat`** — Script para Windows iniciar `api_server.py` em background.


---

## Fluxo de Dados (alto nível) 🔁

1. **Busca**: SQL em `sql_scripts` executado contra o BISP (via ODBC/`pyodbc`).
2. **Processamento**: lógica em `api_server.py` (função `processar_dados_python`) aplica normalizações, validações e inferências por local.
3. **Cache**: resultado salvo em `cache/diagnostico_cache.csv` e `cache_status.json` com timestamp. Decisão entre usar cache ou consultar BISP é feita em `get_dataframe_from_cache_or_bisp()`.
4. **Front-End**: página `/diagnostico` consome dados em `/api/dados-diagnostico` (filtros, ordenação via query params).
5. **Auditoria Manual**: ajustes manuais gravados em `cache/auditoria_copasa.json` são aplicados por `aplicar_auditoria` antes de exibir/exportar.
6. **Sheets**: atualização de planilhas Google (via conta de serviço ou token OAuth) feita nas rotinas `update_google_sheet` (em `main.py` ou `api_server.py`).


---

## Procedimentos Técnicos (rápido) ✅

### 1) Instalação

- Criar e ativar venv:

```bash
python -m venv venvTCO
.\venvTCO\Scripts\activate
pip install -r requirements.txt
```

- Configurar `.env` (na raiz) com:

```
DB_USERNAME="seu_usuario_bisp"
DB_PASSWORD="sua_senha_bisp"
CHAVE_SECRETA="uma_chave_aleatoria_usada_por_main_py"
```

- Colocar credenciais Google:
  - Se usa conta de serviço: `config/credenciais-robo.json` (usado por `api_server.py`).
  - Se usa OAuth: gerar `config/token.json` e `config/credentials.json` (usado por `main.py` quando token está disponível).

- Certificado ODBC para Impala: `config/cacerts.pem` (referenciado em string de conexão).


### 2) Rodando localmente

- Servidor principal (recomendado):

```bash
python api_server.py
# ou no Windows usar iniciar_servidor_oculto.bat para rodar em background
```

- Alternativa (atualizador simples Google Sheets):

```bash
python main.py
# Dispara atualização via: http://localhost:5000/run-update/<CHAVE_SECRETA>
```


### 3) Endpoints úteis

- GET `/` — Status e links do app (ambos os servidores exibem algo similar).
- POST `/iniciar-atualizacao` — (api_server) dispara atualização de planilha/diagnóstico.
- GET `/api/dados-diagnostico` — Retorna JSON com `stats`, `details`, `cache_info` (aceita filtros via query string: `filtro_cia`, `filtro_valido`, `filtro_local`, `filtro_motivo`, `sort_by`, `sort_order`).
- POST `/api/copasa/auditar` — Recebe `{ bos, status }` para salvar auditoria manual.
- GET `/api/copasa/export-xlsx` — Exporta o conjunto filtrado para XLSX.
- POST `/api/compress` — Compressão de PDF via Ghostscript.
- POST `/api/extract-merge` — Extrair páginas e mesclar PDFs.


### 4) Google Sheets — notas práticas

- `main.py` usa OAuth (token.json) — ideal para ambiente local com interação.
- `api_server.py` usa conta de serviço (`credenciais-robo.json`) para servidor sem interação.
- Permissões: conceda edição à conta de serviço no Google Sheet.


### 5) Compressão/GS

- Ghostscript é opcional, mas recomendado para compressão. Defina `GS_BIN` no ambiente se necessário (ex: `gswin64c`).
- Se não estiver no PATH, compress falhará; o código tenta detectar binários conhecidos (`gswin64c`, `gswin32c`, `gs`).


---

## Cache, Auditoria e Recuperação 🔍

- **Arquivos chave**:
  - `cache/diagnostico_cache.csv`: dados de diagnóstico prontos para exibição.
  - `cache/cache_status.json`: timestamp e metadados de quando o cache foi gerado.
  - `cache/auditoria_copasa.json`: mapeamento `{ bos: 'validado_manual'|'invalidado_manual' }`.

- **Quando o cache é atualizado**: se a consulta `SQL_ATUALIZACAO_PATH` indicar que houve updates no BISP ou se o CSV não existir, o app reconsulta e regrava o cache.

- **Restaurar auditoria**: editar `cache/auditoria_copasa.json` é suficiente (usualmente via UI `/api/copasa/auditar`).


---

## Boas práticas e Segurança ⚠️

- NÃO versionar: `config/credenciais-robo.json`, `config/token.json`, qualquer `.env` com credenciais.
- Mantenha `cacerts.pem` seguro e atualizado se a conexão ODBC exigir certificados válidos.
- Chave secreta de `main.py` deve ser forte; não exponha publicamente.
- Ao expor servidor via internet (ngrok ou similar), limite quem pode chamar endpoints que disparam jobs.


---

## Solução de Problemas Rápida 🩺

- Erro ao conectar no BISP: verifique `DB_USERNAME`, `DB_PASSWORD` e `config/cacerts.pem` e se o driver ODBC (Cloudera) está instalado.
- Atualização do Google falha: confira permissões do `credenciais-robo.json` ou presença de `token.json` e escopos corretos (`https://www.googleapis.com/auth/spreadsheets`).
- Compressão de PDF falha: instale Ghostscript e defina `GS_BIN` ou acrescente o binário ao PATH.
- Pagina `/diagnostico` lenta na primeira carga: é normal (consulta ao BISP). Aguarde para que o cache seja gerado.


---

## Como contribuir ✍️

- Atualize ou adicione scripts SQL em `sql_scripts/` e documente a finalidade no topo do arquivo.
- Para alterações em regras de validação, prefira editar `processar_dados_python()` em `api_server.py` (escreva testes rápidos localmente).
- Ao adicionar novas rotas, documente o endpoint nesta mesma documentação.


---

## Arquivos importantes para revisão rápida

- `api_server.py` — lógica principal de produção
- `main.py` — atualizador alternativo/simplificado
- `copasa.py` — funções de auditoria manual
- `sql_scripts/*.sql` — consultas utilizadas
- `templates/diagnostico.html` — JS da UI e interações


---

Se quiser, eu posso:
- Gerar um resumo em formato PDF; ou
- Adicionar seções detalhadas (ex.: diagrama de fluxo de dados, exemplos de payloads de API) — diga qual prefere.

✅ Documento criado: `DOCUMENTACAO_PROJETOS.md`

