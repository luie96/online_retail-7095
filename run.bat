@echo off
chcp 65001 >nul 2>&1
echo ========================================
echo COMP7095 Kaggle online_retail 全流程启动
echo ========================================
echo.
if /i "%~1"=="perf" goto RUN_PERF
if /i "%~1"=="performance" goto RUN_PERF

set "DATA_FILE=online_retail.csv"
if not "%COMP7095_LOCAL_CSV%"=="" set "DATA_FILE=%COMP7095_LOCAL_CSV%"

echo [1/6] 检查 Kaggle 数据集文件...
if not exist "%DATA_FILE%" (
    echo ❌ 未找到数据集文件：%DATA_FILE%
    echo 请将数据集放在当前目录，或设置环境变量 COMP7095_LOCAL_CSV 指定文件名
    pause
    exit /b 1
)
echo ✅ 数据集已找到：%DATA_FILE%
echo.

echo [2/6] 检查系统环境变量...
if "%JAVA_HOME%"=="" (
    echo ❌ JAVA_HOME环境变量未设置
    pause
    exit /b 1
)
if "%HADOOP_HOME%"=="" (
    echo ❌ HADOOP_HOME环境变量未设置
    pause
    exit /b 1
)
echo ✅ JAVA_HOME=%JAVA_HOME%
echo ✅ HADOOP_HOME=%HADOOP_HOME%
echo.

echo [3/6] 检查HDFS进程...
jps | findstr /i "NameNode DataNode SecondaryNameNode" >nul 2>&1
if errorlevel 1 (
    echo ❌ HDFS NameNode/DataNode/SecondaryNameNode未启动
    echo 请先运行 %HADOOP_HOME%\sbin\start-dfs.cmd 启动HDFS
    pause
    exit /b 1
)
echo ✅ HDFS进程已启动
echo [3/6] 校验 HDFS CLI 可用（hdfs dfs -ls /）...
hdfs dfs -ls / >nul 2>&1
if errorlevel 1 (
    echo ❌ HDFS CLI 不可用：无法执行 hdfs dfs -ls /
    echo 请检查 HADOOP_HOME 配置、PATH 以及 HDFS 是否正常启动
    pause
    exit /b 1
)
echo ✅ HDFS CLI 校验通过
echo.

echo [4/6] 检查Python环境...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python未安装或未添加到PATH
    pause
    exit /b 1
)
python --version
echo ✅ Python环境检查通过
echo.

echo [5/6] 检查并安装依赖包...
pip install -r requirements.txt -q
if errorlevel 1 (
    echo ❌ 依赖包安装失败
    pause
    exit /b 1
)
echo ✅ 依赖包检查通过
echo.
python -c "import pyspark; print('PySpark', pyspark.__version__)" 2>nul
if errorlevel 1 echo ⚠️ 无法打印 PySpark 版本，请检查 pip 安装
echo.

echo [6/6] 启动全流程代码...
set "HADOOP_OPTS=-Dio.native.lib.available=false %HADOOP_OPTS%"
set "SPARK_HADOOP_OPTS=-Dio.native.lib.available=false %SPARK_HADOOP_OPTS%"
python main.py
if errorlevel 1 (
    echo ❌ 全流程执行失败，请查看error.log
    pause
    exit /b 1
)
echo.

echo ========================================
echo ✅ Kaggle online_retail.csv 全流程执行成功
echo 📊 结果文件已保存到 ./results/ 目录
echo 📝 错误日志：./error.log
echo ========================================
pause
exit /b 0

:RUN_PERF
echo [性能测试] 将运行 performance_test（耗时较长）...
set "HADOOP_OPTS=-Dio.native.lib.available=false %HADOOP_OPTS%"
set "SPARK_HADOOP_OPTS=-Dio.native.lib.available=false %SPARK_HADOOP_OPTS%"
pip install -r requirements.txt -q
python main.py --perf
if errorlevel 1 (
    echo ❌ 性能测试失败
    pause
    exit /b 1
)
echo ✅ 性能测试完成，见 results\performance_report.md 与 perf_*.png
pause
exit /b 0
