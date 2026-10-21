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
if not defined SPARK_HOME set "SPARK_HOME=C:\spark\spark-3.5.7-bin-hadoop3"
if not exist "%SPARK_HOME%\bin\spark-submit.cmd" (
  echo ERROR: spark-submit.cmd was not found under "%SPARK_HOME%".
  exit /b 1
)
pushd "%PROJECT_ROOT%"
"%SPARK_HOME%\bin\spark-submit.cmd" --master local[2] --driver-memory 3g --conf spark.jars.ivy="%PROJECT_ROOT%\.ivy2" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.sql.legacy.timeParserPolicy=LEGACY --packages org.apache.hudi:hudi-spark3.5-bundle_2.12:1.0.2 "%PROJECT_ROOT%\src\consumption_recommendation.py"
set "EXIT_CODE=%ERRORLEVEL%"
popd
exit /b %EXIT_CODE%
