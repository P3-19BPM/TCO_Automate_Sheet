@echo off

:: Define o diretório de trabalho do script. Isso garante que todos os
:: caminhos relativos (como para a pasta 'config' e 'sql_scripts') funcionem.
set "WORKDIR=E:\TCO\TCO_Automate_Sheet"

:: Caminho para o executável pythonw.exe dentro do seu ambiente virtual
set "PYTHONW_EXE=%WORKDIR%\venvTCO\Scripts\pythonw.exe"

:: Caminho completo para o seu script de API
set "SCRIPT_PATH=%WORKDIR%\api_server.py"

:: Navega para o diretório de trabalho antes de executar.
:: Este é um passo importante para garantir a consistência.
cd /d "%WORKDIR%"

:: Executa o script de forma oculta
:: O título "APIServerTCO" ajuda a identificar o processo.
start "APIServerTCO" "%PYTHONW_EXE%" "%SCRIPT_PATH%"
