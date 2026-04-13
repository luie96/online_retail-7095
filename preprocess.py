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


def ensure_windows_spark_env() -> None:
    """确保 Windows 下 JAVA_HOME、HADOOP_HOME 可见。"""
    for key in ("JAVA_HOME", "HADOOP_HOME"):
        if not os.environ.get(key):
            print(f"[警告] 环境变量 {key} 未设置，PySpark/Hadoop 可能无法在 Windows 下启动。")


def build_spark_session(app_name: str = "KaggleOnlineRetail") -> SparkSession:
    """构建带 Windows 友好配置的 SparkSession。"""
    ensure_windows_spark_env()
    root = Path.cwd()
    wh = root / "spark-warehouse"
    tmp = root / "tmp"
    wh.mkdir(parents=True, exist_ok=True)
    tmp.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", str(wh.resolve()))
        .config("spark.local.dir", str(tmp.resolve()))
        .config("spark.sql.session.timeZone", "Europe/London")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.network.timeout", "600s")
        .config("spark.python.worker.timeout", "600")
        .config("spark.python.use.daemon", "false")
        .config("spark.hadoop.io.native.lib.available", "false")
        .getOrCreate()
    )
    return spark


def _invoice_timestamp(col_name: str = "InvoiceDate"):
    """
    解析发票时间：兼容常见 Kaggle 变体（yyyy-MM-dd HH:mm:ss 与 dd/MM/yyyy HH:mm）。
    tips 中 dd/MM/yyyy 与部分下载包中 yyyy-MM-dd 并存。
    """
    c = F.trim(F.col(col_name))
    return F.coalesce(
        F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"),
        F.to_timestamp(c, "yyyy/MM/dd HH:mm:ss"),
        F.to_timestamp(c, "dd/MM/yyyy HH:mm"),
        F.to_timestamp(c, "dd/MM/yyyy H:mm"),
    )


def preprocess_kaggle_data(spark_df: DataFrame) -> DataFrame:
    """
    Kaggle online_retail.csv 预处理（零售场景）。
    输入参数：spark_df — 原始 Kaggle 数据。
    输出结果：df_cleaned — 清洗后的 DataFrame。
    """
    _ = spark_df.sparkSession
    inv_ts = _invoice_timestamp("InvoiceDate")

    df_dedup = spark_df.dropDuplicates(subset=["InvoiceNo", "StockCode"])
    n0 = spark_df.count()
    n1 = df_dedup.count()
    print(f"✅ 去重前行数：{n0}，去重后行数：{n1}")

    df_no_missing = (
        df_dedup.dropna(
            subset=["InvoiceNo", "StockCode", "Quantity", "InvoiceDate", "UnitPrice"]
        )
        .filter((F.col("Quantity") > 0) & (F.col("UnitPrice") > 0))
        .withColumn("_inv_ts", inv_ts)
        .filter(F.col("_inv_ts").isNotNull())
    )
    print(f"✅ 缺失值/无效数据处理后行数：{df_no_missing.count()}")

    df_time_norm = (
        df_no_missing.withColumn("InvoiceDateTime", F.col("_inv_ts"))
        .withColumn("InvoiceDate", F.date_format(F.col("_inv_ts"), "yyyy-MM-dd"))
        .withColumn("InvoiceHour", F.date_format(F.col("_inv_ts"), "HH"))
        .drop("_inv_ts")
    )

    df_desensitized = df_time_norm.withColumn(
        "CustomerID",
        F.sha2(F.col("CustomerID").cast("string"), 256),
    ).withColumn(
        "Description",
        F.regexp_replace(F.col("Description"), "[^a-zA-Z0-9 ]", ""),
    )

    df_cleaned = df_desensitized.withColumn(
        "TotalAmount",
        F.col("Quantity").cast("double") * F.col("UnitPrice").cast("double"),
    ).select(
        "InvoiceNo",
        "StockCode",
        "Description",
        "Quantity",
        "InvoiceDate",
        "InvoiceHour",
        "InvoiceDateTime",
        "UnitPrice",
        "TotalAmount",
        "CustomerID",
        "Country",
    )

    print("✅ Kaggle online_retail.csv 预处理完成")
    return df_cleaned


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
