# COMP7095 在线零售分布式分析 — 代码说明（Windows）

## 项目架构概览

- **数据层**：本地 `online_retail.csv`（Kaggle Online Retail）。
- **存储层**：HDFS — 原始 CSV、清洗后 Parquet、分析结果 CSV 目录。
- **计算层**：PySpark（`local[*]` 或集群，`spark.default.parallelism` / `spark.sql.shuffle.partitions` 可配置）。
- **分析层**：聚合指标、RFM、FP-Growth 关联规则、用户行为与销售趋势等（纯 Spark DataFrame，无 Pandas 计算回退）。
- **输出层**：本地 `results/*.csv` + HDFS `config.py` 中 `hdfs_results_dir` 下各子目录。

详细分层图见同目录 `architecture.md`（Mermaid）。

## 代码目录结构

```text
./
├── config.py                 # 全局参数（路径、Spark、HDFS、分析阈值）
├── logging_setup.py          # logging 初始化（控制台 + logs/pipeline_YYYYMMDD.log）
├── online_retail.csv         # 原始数据（文件名可通过 COMP7095_LOCAL_CSV 覆盖）
├── data_loader.py            # 从 HDFS 读取 CSV + 字段校验
├── preprocess.py             # PySpark 预处理与衍生字段
├── hdfs_manager.py           # HDFS 目录初始化、上传/下载、读写、存在性检测
├── distributed_processing.py # 分布式分析与结果落盘（本地 + HDFS）
├── performance_test.py       # Pandas vs Spark 性能对比实验
├── main.py                   # 主流程 / --perf 性能测试入口
├── run.bat                   # 一键环境检查 + 主流程；run.bat perf 运行性能实验
├── requirements.txt          # 锁定版本依赖
├── pseudocode.txt            # 全流程伪代码
├── report_supplement.md      # Limitations / Future Work（报告补充）
├── architecture.md           # 分布式架构 Mermaid 图
├── error.log                 # 异常堆栈（失败时）
└── results/                  # 分析 CSV、性能报告与图表
```

## 环境要求

| 组件 | 说明 |
|------|------|
| **Python** | 建议 **3.10–3.12**（3.8+ 需与 PySpark wheel 匹配）。 |
| **JDK** | 64 位 **JDK 8 / 11 / 17**，`JAVA_HOME` 正确。 |
| **Hadoop** | 伪分布式或集群；`HADOOP_HOME`、`winutils.exe`、`hadoop.dll`（Windows）。 |
| **内存** | 建议 ≥ 8 GB（全量 CSV + Spark + JVM）。 |

## 环境变量

| 变量 | 作用 |
|------|------|
| `JAVA_HOME` | JDK 根目录。 |
| `HADOOP_HOME` | Hadoop 根目录，`bin\hdfs.cmd` 可用。 |
| `Path` | 包含 `%JAVA_HOME%\bin`、`%HADOOP_HOME%\bin`。 |

可选覆盖配置（前缀 `COMP7095_`）：`HDFS_ROOT`、`HDFS_RAW`、`SPARK_SHUFFLE_PARTITIONS`、`FP_MIN_SUPPORT` 等，详见 `config.py`。

## HDFS 与 Spark 配置提要

1. 首次部署：`hdfs namenode -format`（谨慎，会清空元数据）。
2. 启动 HDFS：`%HADOOP_HOME%\sbin\start-dfs.cmd`。
3. 确认进程：`jps` 中出现 NameNode / DataNode。
4. Spark 通过 `preprocess.build_spark_session()` 创建；并行度、shuffle 分区、Executor/Driver 内存均来自 `config.py`。

## 一键运行步骤

1. 将 `online_retail.csv` 置于项目根目录（或设置 `COMP7095_LOCAL_CSV`）。
2. 配置 `JAVA_HOME`、`HADOOP_HOME`，启动 HDFS。
3. 双击或在命令行执行 `run.bat`：
   - 检查数据文件、环境变量、HDFS 进程、Python、依赖安装；
   - 运行 `python main.py`。
4. 性能实验（可选）：`run.bat perf` 或 `python main.py --perf`，输出 `results/performance_report.md` 与 `perf_*.png`。

## 主程序流程（main.py）

1. `hdfs_manager.init_hdfs_dir()` 创建项目 HDFS 目录。
2. `upload_local_data_to_hdfs` 上传本地 CSV 至 `hdfs_raw_file`。
3. `data_loader.load_kaggle_ecommerce_data_from_hdfs` 从 HDFS 读入 Spark。
4. `preprocess.preprocess_kaggle_data` 清洗与衍生字段。
5. `write_parquet_to_hdfs` 写入清洗 Parquet；再 **从 HDFS 回读** 供分析使用。
6. `distributed_processing.process_kaggle_retail_data`：本地 `results/` + HDFS 结果目录双写。
7. `hdfs_manager.close_hdfs_client()`，`spark.stop()`。

## 核心模块说明

| 模块 | 职责 |
|------|------|
| `config.py` | 集中配置，避免业务代码硬编码路径与参数。 |
| `hdfs_manager.py` | `init_hdfs_dir`、`upload_local_data_to_hdfs`、`read_data_from_hdfs`、`write_result_to_hdfs`、`check_hdfs_file_exists`、`close_hdfs_client` 等。 |
| `preprocess.py` | 取消单过滤、异常值、完全重复行、CustomerID 非空、Description 填充、时间维度、OrderKey、repartition。 |
| `distributed_processing.py` | 经典 5 张表 + RFM、关联规则、用户行为摘要、首购分布、月度趋势、季节性、国家同比等。 |
| `performance_test.py` | Pandas 基线 vs Spark 多分区、多数据比例耗时与图表。 |

## 实验结果说明

运行成功后，`results/` 包含：

- `daily_sales.csv`、`top20_products.csv`、`country_sales.csv`、`customer_frequency.csv`、`hourly_orders.csv`
- `rfm_segments.csv`、`association_rules.csv`、`user_behavior_summary.csv`
- `first_purchase_distribution.csv`、`monthly_sales_trend.csv`、`product_seasonality.csv`、`country_sales_growth.csv`

HDFS 上在 `hdfs_results_dir` 下为各分析名的子目录（Spark CSV 多 part 结构属正常）。

性能实验额外生成：`performance_report.md`、`perf_spark_partitions.png`、`perf_scale_compare.png`。

## 分析结论（摘要）

- 预处理保证取消单与无效交易剔除，客户维度以非空 `CustomerID` 为主键之一。
- RFM 与关联规则支持用户分层与商品组合运营；月度/国家维度支持趋势与区域策略。
- 性能脚本用于对比单机 Pandas 与 Spark 在不同 shuffle 分区与采样比例下的耗时（具体数值以本机复现为准）。

## 常见问题

- **NativeIO / winutils**：确保 `HADOOP_HOME` 与 `hadoop.dll`；`run.bat` 已注入 `-Dio.native.lib.available=false`。
- **Parquet 写 HDFS 失败**：已内置 Windows 下 pyarrow 回退路径（需安装 `pyarrow`）。
