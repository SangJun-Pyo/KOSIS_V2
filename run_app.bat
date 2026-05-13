@echo off
cd /d %~dp0
set "PYTHONUTF8=1"

if exist ".env.local" (
  for /f "usebackq tokens=1,* delims==" %%A in (".env.local") do (
    if /i not "%%A"=="REM" if not "%%A"=="" set "%%A=%%B"
  )
)

set "PY_CMD="
where py >nul 2>&1
if not errorlevel 1 (
  set "PY_CMD=py -3.13"
) else (
  where python >nul 2>&1
  if not errorlevel 1 (
    set "PY_CMD=python"
  )
)

if "%PY_CMD%"=="" (
  echo [ERROR] Python is not installed or not available on PATH.
  pause
  exit /b 1
)

set "LOCAL_DEPS=%CD%\.deps"
if not exist "%LOCAL_DEPS%" mkdir "%LOCAL_DEPS%"
set "PYTHONPATH=%LOCAL_DEPS%;%PYTHONPATH%"

call %PY_CMD% -c "import requests,pandas,openpyxl,streamlit" >nul 2>&1
if errorlevel 1 (
  echo [INFO] Installing dependencies...
  call %PY_CMD% -m pip install --disable-pip-version-check --target "%LOCAL_DEPS%" -r requirements.txt
  if errorlevel 1 (
    echo [ERROR] Failed to install dependencies.
    pause
    exit /b 1
  )
)

echo [INFO] Launching KOSIS V1...
call %PY_CMD% -m streamlit run app.py --global.developmentMode false
pause
