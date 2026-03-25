@echo off
setlocal

cd /d "%~dp0.."

set "MODE=%~1"
if "%MODE%"=="" set "MODE=preview"

set "LOG_DIR=%~dp0logs"
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set "LOG_FILE=%LOG_DIR%\run_live_job_%MODE%.log"

for /f "usebackq delims=" %%I in (`powershell -NoProfile -Command "(Get-Date).ToString('yyyy-MM-dd HH:mm:ss')"`) do set "RUN_TS=%%I"

>> "%LOG_FILE%" echo ==================================================
>> "%LOG_FILE%" echo [%RUN_TS%] mode=%MODE%
py live\run_live_job.py --mode %MODE% >> "%LOG_FILE%" 2>&1
set "EXITCODE=%ERRORLEVEL%"
>> "%LOG_FILE%" echo [exit_code=%EXITCODE%]
>> "%LOG_FILE%" echo.

endlocal
