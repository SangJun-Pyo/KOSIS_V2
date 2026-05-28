@echo off
cd /d %~dp0
chcp 65001 >nul
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"

if exist ".env" (
  for /f "usebackq tokens=1,* delims==" %%A in (".env") do (
    if /i not "%%A"=="REM" if not "%%A"=="" set "%%A=%%B"
  )
)
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

if not "%PY_CMD%"=="" (
  call %PY_CMD% -c "import sys" >nul 2>&1
  if errorlevel 1 (
    echo [WARN] A Python launcher was found, but no usable Python 3.13 runtime is installed.
    set "PY_CMD="
  )
)

if "%PY_CMD%"=="" (
:install_python
  echo [WARN] Python was not found.
  echo [INFO] Trying to install Python 3.13 with winget...
  where winget >nul 2>&1
  if errorlevel 1 (
    echo [ERROR] winget is not available on this PC.
    echo [INFO] Install Python 3.13 or later, then run this file again.
    echo [INFO] Download: https://www.python.org/downloads/windows/
    pause
    exit /b 1
  )

  winget install --id Python.Python.3.13 --exact --accept-package-agreements --accept-source-agreements
  if errorlevel 1 (
    echo [ERROR] Python installation did not complete.
    echo [INFO] Install Python 3.13 or later manually, then run this file again.
    echo [INFO] Download: https://www.python.org/downloads/windows/
    pause
    exit /b 1
  )

  echo [INFO] Python installation finished. Re-checking Python...
  where py >nul 2>&1
  if not errorlevel 1 (
    set "PY_CMD=py -3.13"
  ) else (
    where python >nul 2>&1
    if not errorlevel 1 (
      set "PY_CMD=python"
    )
  )

  if not "%PY_CMD%"=="" (
    call %PY_CMD% -c "import sys" >nul 2>&1
    if errorlevel 1 (
      set "PY_CMD="
    )
  )

  if "%PY_CMD%"=="" (
    echo [ERROR] Python still was not found in this console.
    echo [INFO] Close this window and run run_app.bat again.
    pause
    exit /b 1
  )
)

call %PY_CMD% -c "import sys" >nul 2>&1
if errorlevel 1 (
  echo [WARN] The detected Python command is not usable.
  set "PY_CMD="
  goto :install_python
)

echo [INFO] Checking pip...
call %PY_CMD% -m pip --version >nul 2>&1
if errorlevel 1 (
  echo [WARN] pip is not available in this Python environment.
  echo [INFO] Trying to bootstrap pip with ensurepip...
  call %PY_CMD% -m ensurepip --upgrade
  if errorlevel 1 (
    echo [ERROR] Failed to bootstrap pip with ensurepip.
    echo [INFO] This usually means Python itself is incomplete or the launcher points to a missing runtime.
    echo [INFO] Reinstalling Python 3.13 with winget is recommended.
    where winget >nul 2>&1
    if not errorlevel 1 (
      echo [INFO] Run this command if needed:
      echo [INFO] winget install --id Python.Python.3.13 --exact --accept-package-agreements --accept-source-agreements
    )
    echo [INFO] Manual download: https://www.python.org/downloads/windows/
    pause
    exit /b 1
  )

  call %PY_CMD% -m pip --version >nul 2>&1
  if errorlevel 1 (
    echo [ERROR] pip is still not available after ensurepip.
    echo [INFO] Reinstall Python 3.13 or later with pip included, then run this file again.
    echo [INFO] Manual download: https://www.python.org/downloads/windows/
    pause
    exit /b 1
  )
)

call %PY_CMD% -c "import requests,pandas,openpyxl,streamlit" >nul 2>&1
if errorlevel 1 (
  echo [INFO] Installing or updating required packages...
  call %PY_CMD% -m pip install --disable-pip-version-check --upgrade -r requirements.txt
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
)

if "%KOSIS_API_KEY%"=="" (
  echo [WARN] KOSIS_API_KEY is not set.
  echo [WARN] The app can open, but real KOSIS runs may fail.
  echo [INFO] Add this line to .env:
  echo [INFO] KOSIS_API_KEY=YOUR_REAL_API_KEY
)

echo [INFO] Starting KOSIS Dashboard...
set "STREAMLIT_LAUNCHER=%TEMP%\kosis_streamlit_launcher.bat"
(
  echo @echo off
  echo title KOSIS Dashboard Console - close with Ctrl+C
  echo cd /d "%CD%"
  echo chcp 65001 ^>nul
  echo set "PYTHONUTF8=1"
  echo set "PYTHONIOENCODING=utf-8"
  echo echo [INFO] KOSIS Dashboard is running.
  echo echo [INFO] Press Ctrl+C in this window to close the dashboard.
  echo echo [INFO] If a confirmation prompt appears, press Y.
  echo call %PY_CMD% -m streamlit run app.py --global.developmentMode false --server.headless true
  echo exit /b %%errorlevel%%
) > "%STREAMLIT_LAUNCHER%"

start "KOSIS Dashboard Console" /min "%STREAMLIT_LAUNCHER%"
start "" "http://localhost:8501"
echo [INFO] Dashboard console started minimized.
echo [INFO] Your browser should open automatically.
echo [INFO] To stop the dashboard, press Ctrl+C in the dashboard console window.
exit /b 0
