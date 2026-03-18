@echo off
setlocal

REM ===== CONFIG =====
set "APP_DIR=E:\TCO\TCO_Automate_Sheet"
set "VENV_PY=%APP_DIR%\venvTCO\Scripts\python.exe"
set "SCRIPT=%APP_DIR%\api_server.py"
set "LOG_DIR=%APP_DIR%\server_logs"
set "PORT=8088"

REM ===== PREP =====
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

REM Timestamp seguro (sem / : espaço , .)
set "TS=%DATE:/=-%_%TIME::=-%"
set "TS=%TS: =0%"
set "TS=%TS:,=%"
set "TS=%TS:.=%"
set "LOG=%LOG_DIR%\flask_%TS%.log"

REM Rotação simples: mantém só 10 logs
for /f "skip=10 delims=" %%F in ('dir /b /o-d "%LOG_DIR%\flask_*.log" 2^>nul') do del "%LOG_DIR%\%%F" >nul 2>&1

pushd "%APP_DIR%"

REM ===== START oculto (sem caret/escape, tudo numa linha) =====
REM Redireção fica dentro das aspas do cmd /c
start "" /min cmd /c ""%VENV_PY%" -X utf8 -u "%SCRIPT%" 1> "%LOG%" 2>&1"

echo Aguardando o servidor iniciar...
timeout /t 4 >nul

REM Testa rapidamente se a /status responde (se não tiver /status, troque por "/")
powershell -NoProfile -ExecutionPolicy Bypass ^
  "try{ $r=Invoke-WebRequest -Uri 'http://127.0.0.1:%PORT%/status' -UseBasicParsing -TimeoutSec 3; if($r.StatusCode -eq 200){'OK'} else{'ERR'} }catch{'ERR'}" > "%TEMP%\__ping8088.txt"

findstr /C:"OK" "%TEMP%\__ping8088.txt" >nul
if %ERRORLEVEL%==0 (
  echo Servidor ativo em http://10.14.56.162:%PORT%/
  start "" "http://10.14.56.162:%PORT%/pdf"
) else (
  echo [ALERTA] Nao consegui confirmar o servidor. Veja o log:
  echo %LOG%
)

del "%TEMP%\__ping8088.txt" >nul 2>&1
popd
endlocal
