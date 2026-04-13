# -*- coding: utf-8 -*-
"""
模块2：Kaggle online_retail.csv 零售场景预处理（PySpark）
"""

from __future__ import annotations

import os
from pathlib import Path

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from config import CONFIG


def ensure_windows_spark_env() -> None:
    """确保 Windows 下 JAVA_HOME、HADOOP_HOME 可见。"""
    for key in ("JAVA_HOME", "HADOOP_HOME"):
        if not os.environ.get(key):
            print(f"[警告] 环境变量 {key} 未设置，PySpark/Hadoop 可能无法在 Windows 下启动。")


def build_spark_session(app_name: str | None = None) -> SparkSession:
    """
    构建 SparkSession，并行度 / shuffle 分区 / 内存等从 config 读取，兼容 local 与集群。

    Args:
        app_name: 应用名；默认使用 CONFIG.spark_app_name。

    Returns:
        SparkSession。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    ensure_windows_spark_env()
    root = Path.cwd()
    wh = root / "spark-warehouse"
    tmp = root / "tmp"
    wh.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)

    an = app_name or CONFIG.spark_app_name
    spark = (
        SparkSession.builder.appName(an)
        .master(CONFIG.spark_master)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(wh.resolve()))
        .config("spark.local.dir", str(tmp.resolve()))
        .config("spark.sql.session.timeZone", CONFIG.spark_sql_timezone)
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.network.timeout", "600s")
        .config("spark.python.worker.timeout", "600")
        .config("spark.python.use.daemon", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.default.parallelism", str(CONFIG.spark_default_parallelism))
        .config("spark.sql.shuffle.partitions", str(CONFIG.spark_sql_shuffle_partitions))
        .config("spark.executor.memory", CONFIG.spark_executor_memory)
        .config("spark.executor.cores", str(CONFIG.spark_executor_cores))
        .config("spark.driver.memory", CONFIG.spark_driver_memory)
        .getOrCreate()
    )
    log.info(
        "SparkSession 已创建：master=%s parallelism=%s shufflePartitions=%s",
        CONFIG.spark_master,
        CONFIG.spark_default_parallelism,
        CONFIG.spark_sql_shuffle_partitions,
    )
    return spark


def _invoice_timestamp(col_name: str = "InvoiceDate"):
    """
    解析发票时间：兼容常见 Kaggle 变体（yyyy-MM-dd HH:mm:ss 与 dd/MM/yyyy HH:mm）。
    """
    c = F.trim(F.col(col_name).cast("string"))
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy/MM/dd HH:mm:ss"),
        F.to_timestamp(c, "dd/MM/yyyy HH:mm"),
        F.to_timestamp(c, "dd/MM/yyyy H:mm"),
    )


def _log_count(df: DataFrame, label: str) -> int:
    from logging_setup import get_logger

    n = df.count()
    get_logger(__name__).info("%s：%s 行", label, n)
    print(f"✅ {label}：{n} 行")
    return n


def preprocess_kaggle_data(spark_df: DataFrame) -> DataFrame:
    """
    Kaggle online_retail.csv 全量预处理。

    顺序：取消单过滤 → 异常值过滤 → 完全重复行删除 → CustomerID 非空、Description 填充
    → 时间解析与衍生字段 → 类型规范。

    Args:
        spark_df: 自 HDFS 读入的原始 DataFrame。

    Returns:
        清洗后的 DataFrame。

    Raises:
        AnalysisException: Spark 分析异常。
    """
    _ = spark_df.sparkSession
    inv_ts = _invoice_timestamp("InvoiceDate")

    _log_count(spark_df, "原始行数")

    df1 = spark_df.filter(
        ~F.trim(F.col("InvoiceNo").cast("string")).startswith("C")
    )
    _log_count(df1, "过滤取消单（InvoiceNo 以 C 开头）后")

    df2 = df1.filter((F.col("Quantity").cast("double") > 0) & (F.col("UnitPrice").cast("double") > 0))
    _log_count(df2, "过滤 Quantity/UnitPrice 异常值后")

    df3 = df2.dropDuplicates()
    _log_count(df3, "删除完全重复行后")

    desc_clean = F.when(
        F.col("Description").isNull() | (F.trim(F.col("Description").cast("string")) == ""),
        F.lit("Unknown Product"),
    ).otherwise(F.col("Description").cast("string"))

    df4 = (
        df3.filter(F.col("CustomerID").isNotNull())
        .withColumn("Description", desc_clean)
        .withColumn("CustomerID", F.col("CustomerID").cast("string"))
    )
    _log_count(df4, "剔除 CustomerID 空值并填充 Description 后")

    df5 = df4.withColumn("_inv_ts", inv_ts).filter(F.col("_inv_ts").isNotNull())
    _log_count(df5, "解析 InvoiceDate 成功（时间戳非空）后")

    ts = F.col("_inv_ts")
    # Spark dayofweek: 1=Sunday, 7=Saturday
    dow = F.dayofweek(ts)
    is_weekend = F.when((dow == 1) | (dow == 7), F.lit(1)).otherwise(F.lit(0))

    df6 = (
        df5.withColumn("InvoiceDateTime", ts.cast("timestamp"))
        .withColumn("InvoiceDate", F.date_format(ts, "yyyy-MM-dd"))
        .withColumn("InvoiceHour", F.date_format(ts, "HH"))
        .withColumn("InvoiceYear", F.year(ts))
        .withColumn("InvoiceMonth", F.month(ts))
        .withColumn("InvoiceDay", F.dayofmonth(ts))
        .withColumn("InvoiceWeekday", dow)
        .withColumn("IsWeekend", is_weekend)
        .withColumn(
            "OrderKey",
            F.concat_ws("|", F.trim(F.col("InvoiceNo").cast("string")), F.col("CustomerID")),
        )
        .withColumn("Quantity", F.col("Quantity").cast("long"))
        .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
        .withColumn(
            "TotalAmount",
            F.col("Quantity").cast("double") * F.col("UnitPrice").cast("double"),
        )
        .drop("_inv_ts")
    )

    n_part = max(1, int(CONFIG.spark_repartition_n))
    df_out = df6.repartition(n_part, F.col("CustomerID"))

    _log_count(df_out, "衍生字段与 repartition 后（最终）")
    print("✅ Kaggle online_retail.csv 预处理完成")
    return df_out


def run_preprocess(spark: SparkSession, df: DataFrame) -> DataFrame:
    """兼容旧接口名：等价于 preprocess_kaggle_data。"""
    try:
        return preprocess_kaggle_data(df)
    except AnalysisException as e:
        raise AnalysisException(f"预处理阶段发生 AnalysisException：{e}") from e
    except Py4JJavaError as e:
        raise RuntimeError(
            "预处理阶段 Java/PySpark 连接或 JVM 报错，"
            f"请检查 JAVA_HOME、HADOOP_HOME 与 Spark 版本。详情：{e}"
        ) from e
