@echo off

REM ============================================
REM CONFIGURACIÓN DEL PROYECTO
REM ============================================

set PROJECT_DIR=C:\Users\msami\Desktop\Pruebas Python Scrapper\ctcVendorController
set PYTHON_EXE=C:\Users\msami\AppData\Local\Programs\Python\Python313\python.exe
set LOG_DIR=%PROJECT_DIR%\logs

REM ============================================
REM PREPARAR LOGS
REM ============================================

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo =========================================== >> "%LOG_DIR%\run_megatone.log"
echo INICIO RUN MEGATONE - %DATE% %TIME% >> "%LOG_DIR%\run_megatone.log"

REM ============================================
REM EJECUCIÓN
REM ============================================

cd /d "%PROJECT_DIR%"

"%PYTHON_EXE%" RunAll\RunAllMegatone.py >> "%LOG_DIR%\run_megatone.log" 2>&1

REM ============================================
REM FIN
REM ============================================

echo FIN RUN MEGATONE - %DATE% %TIME% >> "%LOG_DIR%\run_megatone.log"