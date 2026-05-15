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
  echo [INFO] Python 3.13 or later install is recommended.
  pause
  exit /b 1
)

set "LOCAL_DEPS=%CD%\.deps"
if not exist "%LOCAL_DEPS%" mkdir "%LOCAL_DEPS%"
set "PYTHONPATH=%LOCAL_DEPS%;%PYTHONPATH%"

echo [INFO] Checking pip...
call %PY_CMD% -m pip --version >nul 2>&1
if errorlevel 1 (
  echo [ERROR] pip is not available in this Python environment.
  pause
  exit /b 1
)

echo [INFO] Installing or updating dependencies into .deps...
call %PY_CMD% -m pip install --disable-pip-version-check --target "%LOCAL_DEPS%" --upgrade -r requirements.txt
if errorlevel 1 (
  echo [ERROR] Failed to install dependencies.
  pause
  exit /b 1
)

call %PY_CMD% -c "import requests,pandas,openpyxl,streamlit" >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Dependency verification failed after installation.
  pause
  exit /b 1
)

if "%KOSIS_API_KEY%"=="" (
  echo [WARN] KOSIS_API_KEY is not set.
  echo [WARN] The app will open, but real KOSIS collection runs may fail.
  echo [INFO] You can create a .env.local file with:
  echo [INFO] KOSIS_API_KEY=YOUR_REAL_API_KEY
)

echo [INFO] Launching 한국지역고용연구소...
call %PY_CMD% -m streamlit run app.py --global.developmentMode false
pause
