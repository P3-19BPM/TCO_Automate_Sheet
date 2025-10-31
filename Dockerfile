# 1. Imagem Base: Começamos com uma imagem oficial do Python
FROM python:3.11-slim

# 2. Instalação de Dependências do Sistema
# O driver ODBC do Cloudera/Impala precisa de algumas bibliotecas do Linux.
# E também precisamos das ferramentas para compilar o pyodbc.
RUN apt-get update && apt-get install -y \
    gnupg \
    unixodbc-dev \
    g++ \
    && rm -rf /var/lib/apt/lists/*
    
# --- NOVO PASSO: INSTALAR O DRIVER CLOUDERA ---
# Copia o driver .deb para dentro da imagem
# COPY Certificados/clouderaimpalaodbc_2.6.11.1011-2_amd64.deb .
COPY config/clouderaimpalaodbc_2.6.11.1011-2_amd64.deb .
# Instala o driver (e -f install corrige quaisquer dependências quebradas)
RUN dpkg -i clouderaimpalaodbc_2.6.11.1011-2_amd64.deb || apt-get -f install -y
# --- FIM DO NOVO PASSO ---

# 3. Configura o diretório de trabalho dentro do contêiner
WORKDIR /app

# 4. Copia o arquivo de requisitos e instala as bibliotecas Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copia todos os arquivos do projeto para dentro do contêiner
COPY . .

# 6. Expõe a porta que o Flask usará
EXPOSE 5000

# 7. Comando para rodar a aplicação quando o contêiner iniciar
CMD ["python", "main.py"]