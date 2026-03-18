# Automação e Análise de Registros TCO

Este projeto automatiza a coleta de dados de Termos Circunstanciados de Ocorrência (TCO) do banco de dados BISP e fornece duas funcionalidades principais através de um servidor web local:

1.  **Atualização de Planilha Google Sheets:** Um processo que pode ser disparado por uma interface web para atualizar uma planilha com dados recentes.
2.  **Painel de Diagnóstico Interativo:** Uma página web para análise detalhada dos registros, com estatísticas, filtros dinâmicos e exportação de dados.

## Funcionalidades

### Painel de Diagnóstico (`/diagnostico`)

O coração do projeto. Uma ferramenta de análise completa acessível por qualquer pessoa na mesma rede.

-   **Dashboard de Estatísticas:** Visão geral do cumprimento de metas por local, com indicadores visuais (cores) para metas atingidas ou não.
-   **Tabela de Detalhes Otimizada:** Exibe apenas as informações mais relevantes, com coloração de linhas para identificar rapidamente registros válidos e inválidos.
-   **Filtros Combinados:**
    -   Filtro por **Companhia da PM** (42ª ou 47ª).
    -   Filtro por **Status** (Todos, Válidos, Inválidos).
    -   Filtro por **Local** (menu suspenso).
    -   Filtro textual por **Motivo da Invalidade**.
-   **Ordenação Dinâmica:** Clique no cabeçalho de qualquer coluna na tabela de detalhes para ordenar os dados.
-   **Exportação para CSV:** Baixe os dados que estão sendo exibidos (respeitando todos os filtros) com um único clique.
-   **Cache Inteligente:** A primeira carga do dia pode ser lenta, pois busca dados do BISP. As cargas seguintes são quase instantâneas, pois os dados são lidos de um cache local. O cache é invalidado automaticamente se o BISP for atualizado.

### Atualizador de Planilha (`/`)

Interface simples para disparar a atualização de uma planilha Google Sheets com dados do BISP.

-   **Feedback em Tempo Real:** A página exibe o status atual do processo (ex: "Conectando ao banco...", "Atualizando planilha...", "Sucesso!").
-   **Execução em Background:** O processo de atualização roda em segundo plano, permitindo que você feche a janela do navegador sem interrompê-lo.

## Estrutura do Projeto

```
TCO_AUTOMATE_SHEET/
├── cache/                # (Ignorado pelo Git) Armazena dados em cache para performance
├── config/               # Arquivos de configuração (certificados, credenciais)
├── sql_scripts/          # Scripts SQL usados pela aplicação
│   ├── atualizacao.sql
│   ├── diagnostico_tco.sql
│   └── registros_tco_2025.sql
├── templates/            # Arquivos HTML para a interface web
│   ├── diagnostico.html
│   └── status.html
├── venvTCO/              # (Ignorado pelo Git) Ambiente virtual Python
├── .env                  # (Ignorado pelo Git) Variáveis de ambiente (usuário/senha do banco)
├── .gitignore            # Define quais arquivos e pastas o Git deve ignorar
├── api_server.py         # O servidor web Flask que controla toda a aplicação
├── iniciar_servidor_oculto.bat # Script para iniciar o servidor em segundo plano no Windows
└── requirements.txt      # Lista de dependências Python do projeto
```

## Instalação e Configuração

Siga estes passos para configurar o ambiente e rodar o projeto.

### 1. Pré-requisitos

-   Python 3.8 ou superior.
-   Acesso à rede onde o banco de dados BISP está disponível.
-   Credenciais de acesso ao BISP.
-   Um arquivo de credenciais de conta de serviço do Google (`.json`) para acesso à planilha.

### 2. Configuração do Ambiente

1.  **Clone o repositório:**
    ```bash
    git clone <url-do-seu-repositorio>
    cd TCO_AUTOMATE_SHEET
    ```

2.  **Crie e ative o ambiente virtual:**
    ```bash
    python -m venv venvTCO
    .\venvTCO\Scripts\activate
    ```

3.  **Instale as dependências:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure as credenciais:**
    -   Crie um arquivo chamado `.env` na raiz do projeto e adicione suas credenciais do BISP:
        ```
        DB_USERNAME="seu_usuario_bisp"
        DB_PASSWORD="sua_senha_bisp"
        ```
    -   Coloque o arquivo de credenciais do Google (`credenciais-robo.json`) dentro da pasta `config/`.

## Como Usar

### Iniciando o Servidor

Para iniciar o servidor de forma que ele continue rodando mesmo após fechar o terminal, execute o script:

```bash
iniciar_servidor_oculto.bat
```

Isso iniciará o `api_server.py` em segundo plano. O servidor estará acessível na porta `8088`.

### Acessando as Ferramentas

-   **Painel de Diagnóstico:**
    Abra o navegador e acesse: `http://10.14.56.162:8088/diagnostico`

-   **Atualizador de Planilha:**
    Abra o navegador e acesse: `http://10.14.56.162:8088/`

### Parando o Servidor

Como o servidor está rodando em segundo plano, para pará-lo você precisará usar o Gerenciador de Tarefas do Windows:

1.  Abra o Gerenciador de Tarefas (Ctrl+Shift+Esc ).
2.  Vá para a aba "Detalhes".
3.  Procure por um processo chamado `pythonw.exe`.
4.  Selecione-o e clique em "Finalizar tarefa".
