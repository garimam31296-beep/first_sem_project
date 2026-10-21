@echo off
setlocal
set "PYSPARK_PYTHON=C:\Users\LENOVO\AppData\Local\Programs\Python\Python312\python.exe"
set "PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%"
if not exist "%PYSPARK_PYTHON%" (
  echo ERROR: Python was not found at "%PYSPARK_PYTHON%".
  exit /b 1
)
set "PROJECT_ROOT=%~dp0.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"
call "%PROJECT_ROOT%\scripts\seller_catalog_spark_submit.cmd"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%PROJECT_ROOT%\scripts\company_sales_spark_submit.cmd"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%PROJECT_ROOT%\scripts\competitor_sales_spark_submit.cmd"
if errorlevel 1 exit /b %ERRORLEVEL%
call "%PROJECT_ROOT%\scripts\recommendations_spark_submit.cmd"
exit /b %ERRORLEVEL%
