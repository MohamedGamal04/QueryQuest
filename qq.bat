@echo off
setlocal

REM Run QueryQuest from the repository root on Windows.
cd /d "%~dp0"

if exist ".venv\Scripts\qq.exe" (
  ".venv\Scripts\qq.exe" %*
  exit /b %ERRORLEVEL%
)

if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" -m queryquest.app %*
  exit /b %ERRORLEVEL%
)

echo QueryQuest launcher could not find a local virtual environment.
echo.
echo From this folder, run:
echo   py -3.12 -m venv .venv
echo   .venv\Scripts\python -m pip install -U pip
echo   .venv\Scripts\python -m pip install -e .
echo.
exit /b 1
