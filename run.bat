@echo off
chcp 65001 >nul 2>&1
echo ========================================
echo COMP7095 Kaggle online_retail 全流程启动
echo ========================================
echo.

echo [1/6] 检查 Kaggle online_retail.csv 数据集...
if not exist "online_retail.csv" (
    echo ❌ 未找到 Kaggle 数据集文件：online_retail.csv
    echo 请将数据集放在当前目录，文件名严格为 online_retail.csv
    pause
    exit /b 1
)
echo ✅ Kaggle online_retail.csv 数据集已找到
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
