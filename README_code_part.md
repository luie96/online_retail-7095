# COMP7095 电商大数据项目 — 代码部分说明（纯 Windows）

## 代码目录结构

```text
./
├── online_retail.csv             # Kaggle Online Retail 原始 CSV（文件名严格一致）
├── data_loader.py                # 模块1：Kaggle 数据读取与校验
├── preprocess.py                 # 模块2：零售场景 PySpark 预处理
├── hdfs_manager.py               # 模块3：HDFS 管理（subprocess + 权限修复）
├── distributed_processing.py     # 模块4：零售指标聚合与结果 CSV
├── main.py                       # 模块5：全流程入口
├── run.bat                       # Windows 一键运行（含 online_retail.csv 检查）
├── requirements.txt              # Python 依赖
├── pseudocode.txt                # 学术风格伪代码（历史稿，可与当前业务略有不一致）
├── error.log                     # 运行失败时写入堆栈；成功时保持为空
└── results/                      # 5 个 Kaggle 专属结果 CSV
```

## 环境要求（纯 Windows）

### 硬件建议

- **内存**：建议 ≥ 8 GB（PySpark `local[*]` 与 JVM 会占用较多内存）。
- **磁盘**：预留数 GB 用于 Spark 临时目录、HDFS 本地数据与结果输出。

### 软件要求

| 组件 | 说明 |
|------|------|
| **JDK** | **64 位 JDK 8 / 11 / 17**（需 `javac`，不能仅为 JRE）。 |
| **Hadoop + winutils** | 伪分布式 Hadoop（示例：3.3.x），`%HADOOP_HOME%\bin` 下需有 **winutils.exe**、**hadoop.dll**；建议将 **hadoop.dll** 复制到 `C:\Windows\System32` 以减少权限/依赖问题。 |
| **Python** | **3.8–3.11** 为课程最稳妥区间。 **3.12+** 请使用 **`requirements.txt` 中的 PySpark 3.5.x**（3.4 在 3.13 上会缺少 `typing.io`）；并建议安装 **`setuptools`** 以满足部分依赖对 `distutils` 的兼容。 |

## 环境变量配置

| 变量 | 作用 |
|------|------|
| `JAVA_HOME` | 指向 JDK 根目录（例如 `C:\Program Files\Java\jdk8`）。 |
| `HADOOP_HOME` | 指向 Hadoop 解压目录（例如 `C:\hadoop-3.3.6`）。 |
| `Path` | 需包含 `%JAVA_HOME%\bin` 与 `%HADOOP_HOME%\bin`。 |

在 **PowerShell** 中可临时查看：

```powershell
echo $env:JAVA_HOME
echo $env:HADOOP_HOME
java -version
javac -version
hdfs version
```

## Hadoop + winutils 环境准备（纯 Windows）

### 下载与解压

1. 从 Apache 镜像站下载 **Hadoop 二进制包**（与课程要求版本一致，例如 3.3.x）。
2. 解压到短路径目录（避免空格与中文路径），例如 `D:\app\hadoop\hadoop-3.3.5`。

### winutils 配置

1. 获取与 **Hadoop 主版本匹配** 的 **winutils** 与 **hadoop.dll**（社区常见做法：对应版本的 `winutils` 仓库或课程提供包）。
2. 将 **winutils.exe**、**hadoop.dll** 放入 `%HADOOP_HOME%\bin`。
3. 将 **hadoop.dll** 再复制一份到 `C:\Windows\System32`（管理员权限），可降低原生库加载失败概率。

### HDFS 格式化（首次）

```bat
hdfs namenode -format
```

> 警告：格式化会清空 NameNode 元数据，仅在首次或明确需要重建集群时执行。

### HDFS 启动 / 停止

```bat
%HADOOP_HOME%\sbin\start-dfs.cmd
%HADOOP_HOME%\sbin\stop-dfs.cmd
```

**Web UI**：Hadoop 3.x NameNode 一般为 `http://localhost:9870`（2.x 常见为 `50070`）。

可用 `jps` 查看是否包含 **NameNode**、**DataNode**、**SecondaryNameNode**。

## 3. 数据准备（Kaggle online_retail.csv）

### 3.1 数据集来源

- **名称**：Online Retail Dataset（零售电商数据集）
- **来源**：Kaggle 公开数据集（示例页面：<https://www.kaggle.com/datasets/vijayuv/onlineretail>）
- **场景**：英国某在线零售企业的交易数据，包含订单、商品、客户、地域信息
- **规模**：约 54 万条交易记录，覆盖 2010–2011 年（以具体下载文件为准）

### 3.2 核心字段说明

| 字段名 | 类型 | 含义 | 业务说明 |
|--------|------|------|----------|
| InvoiceNo | 字符串 | 订单编号 | 唯一标识一笔订单，`C` 开头一般为取消单 |
| StockCode | 字符串 | 商品编码 | 唯一标识一个商品 |
| Description | 字符串 | 商品描述 | 商品名称 |
| Quantity | 整数 | 购买数量 | 行项目数量 |
| InvoiceDate | 日期时间 | 订单日期时间 | 常见为 `yyyy-MM-dd HH:mm:ss`；代码中同时兼容 `dd/MM/yyyy HH:mm` |
| UnitPrice | 浮点数 | 商品单价 | 单位：英镑 |
| CustomerID | 数值 | 客户编号 | 唯一标识客户（允许缺失，预处理前会参与脱敏） |
| Country | 字符串 | 国家/地区 | 地域分析 |

### 3.3 数据放置要求

- 必须将数据集文件命名为 **`online_retail.csv`**
- 放在项目根目录（与 **`main.py`**、**`run.bat`** 同级）
- 无需解压，直接使用原始 CSV
- **`data_loader.py`** 使用 **`latin-1`** 编码读取，若遇编码错误请确认文件与编码一致

## 运行步骤（纯 Windows）

### 一键运行（推荐）

1. 确认 **HDFS 已启动**（`start-dfs.cmd`）。
2. 双击或在 `cmd` 中执行：

```bat
run.bat
```

脚本将依次检查 **`online_retail.csv` 是否存在**、`JAVA_HOME` / `HADOOP_HOME`、HDFS 进程、Python，并 `pip install -r requirements.txt`，最后执行 `python main.py`。

### 手动分步运行

```bat
pip install -r requirements.txt
python main.py
```

## 结果说明

### 结果目录（Kaggle 零售指标）

成功运行后，在 **`./results/`** 下生成下列文件（均带表头、单文件 CSV）：

| 文件 | 说明 |
|------|------|
| `daily_sales.csv` | 按 `InvoiceDate`：日订单数、日销售额、日销售总量 |
| `top20_products.csv` | 按销量 Top20：商品编码、描述、总销量、总销售额 |
| `country_sales.csv` | 按国家/地区：订单数、销售额、客户数 |
| `customer_frequency.csv` | 按客户：购买频次、总消费、平均订单金额（基于行级 `TotalAmount` 聚合） |
| `hourly_orders.csv` | 按小时：订单数、销售额 |

聚合由 **PySpark** 完成；写出使用 **`toPandas()` → 单文件 CSV**，避免 Spark 默认 `write.csv` 产生 `part-*` 子目录及 Windows 本地 NativeIO 问题。

### HDFS 路径（由 `main.py` 写入）

- 原始 CSV：`/ecommerce/raw/kaggle_online_retail.csv`
- 预处理 Parquet：`/ecommerce/processed/kaggle_cleaned_data/`

可选环境变量 **`COMP7095_HDFS_DEFAULT`**（如 `hdfs://localhost:9000`）可在 `hdfs_manager` 中覆盖默认 NameNode 地址探测。

## 异常排查（Windows）

### Java / PySpark 连接错误

- 确认 **`JAVA_HOME` 为 JDK** 且 `Path` 含 `%JAVA_HOME%\bin`。
- **`java -version` 与 `javac -version` 主版本一致**。
- **不要**在 JDK 8 环境下安装 **PySpark 4.x**（会要求更高版本 class file）。
- 若出现 **`Python worker failed to connect back`**（多出现在聚合结果 `toPandas`）：可尝试 **Python 3.10/3.11**、或降低并行度 `local[2]`，或将指标改为纯 `collectAsList` JVM 路径（需自行改代码）。
- 尝试降低并行度：在代码中将 `local[*]` 改为 `local[2]`（需改 `preprocess.build_spark_session`）。

### HDFS 命令执行失败

- 确认 **`HADOOP_HOME`** 与 **`start-dfs.cmd`** 已执行。
- 在命令行手动测试：`hdfs dfs -ls /`
- 查看 `error.log` 中的 Python 堆栈与 Hadoop 日志。

### winutils / 权限错误

- 确认 **`winutils.exe` 与 `hadoop.dll`** 在 `%HADOOP_HOME%\bin`。
- 代码在上传/写入失败时会尝试 **`hdfs dfs -chmod -R 777`** 及 **`winutils chmod`**（见 `hdfs_manager._try_fix_permissions`）。
- 仍失败时，以**管理员**身份打开终端重试，或检查杀毒软件拦截。

### 中文乱码

- `run.bat` 已 `chcp 65001`；`main.py` 会尝试将 **stdout/stderr 设为 UTF-8**。
- 若 Excel 打开 CSV 乱码，可用 VS Code / Notepad++ 选择 **UTF-8** 打开，或在 Excel 导入时指定编码。

---

## 课程合规提示

- 伪代码见 **`pseudocode.txt`**（ACM 风格；若仍为天池版伪代码，可与当前 Kaggle 流水线对照理解）。
- 当前流水线为 **Kaggle Online Retail**：预处理为 **订单行去重、缺失与无效量价过滤、时间解析、客户 ID 哈希、TotalAmount**；与天池用户行为时间窗无关。
