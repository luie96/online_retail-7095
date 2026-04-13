# -*- coding: utf-8 -*-
"""
模块1：从 HDFS 读取 Kaggle online_retail.csv（PySpark），字段校验。
"""

from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession

import hdfs_manager
from config import CONFIG

# Kaggle Online Retail 标准字段
KAGGLE_REQUIRED_FIELDS: List[str] = [
    "InvoiceNo",
    "StockCode",
    "Description",
    "Quantity",
    "InvoiceDate",
    "UnitPrice",
    "CustomerID",
    "Country",
]


def normalize_local_path(csv_path: str) -> str:
    """
    将 CSV 路径规范化为绝对路径（Windows 反斜杠安全）。

    Args:
        csv_path: 相对或绝对路径。

    Returns:
        绝对路径字符串。
    """
    from pathlib import Path

    p = Path(csv_path)
    if not p.is_absolute():
        p = Path.cwd() / p
    return str(p.resolve())


def validate_spark_columns(df: DataFrame) -> bool:
    """
    校验 DataFrame 是否包含 Kaggle 数据集必填列。

    Args:
        df: Spark DataFrame。

    Returns:
        字段完整为 True，否则 False。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    cols = set(df.columns)
    missing = [f for f in KAGGLE_REQUIRED_FIELDS if f not in cols]
    if missing:
        log.error("缺失必填字段：%s", missing)
        print(f"❌ 缺失 Kaggle 数据集必填字段：{missing}")
        return False
    log.info("Kaggle 字段校验通过")
    print("✅ Kaggle online_retail.csv 字段校验通过")
    return True


def load_kaggle_ecommerce_data_from_hdfs(
    spark: SparkSession,
    hdfs_csv_path: str | None = None,
) -> Optional[DataFrame]:
    """
    从 HDFS 读取 Kaggle online_retail.csv 并完成字段校验。

    Args:
        spark: SparkSession。
        hdfs_csv_path: HDFS 上 CSV 路径；默认 CONFIG.hdfs_raw_file。

    Returns:
        PySpark DataFrame；失败返回 None。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    path = hdfs_csv_path or CONFIG.hdfs_raw_file
    try:
        if not hdfs_manager.check_hdfs_file_exists(path):
            log.error("HDFS 上未找到数据文件：%s", path)
            print(f"❌ HDFS 上未找到文件：{path}，请先上传原始 CSV")
            return None

        spark_df = hdfs_manager.read_data_from_hdfs(path, spark, fmt="csv")
        n = spark_df.count()
        log.info("已从 HDFS 读取数据集，行数：%s", n)
        print(f"✅ 成功从 HDFS 读取 Kaggle 数据集，全量数据行数：{n}")

        if not validate_spark_columns(spark_df):
            return None
        return spark_df
    except Exception as e:
        log.exception("从 HDFS 读取数据集失败：%s", e)
        print(f"❌ 从 HDFS 读取 Kaggle 数据集失败：{e}")
        return None


def load_kaggle_ecommerce_data(
    spark: SparkSession,
    file_path: str = "./online_retail.csv",
) -> Optional[DataFrame]:
    """
    兼容旧接口：改为从 HDFS 读取（忽略本地 file_path，使用 CONFIG.hdfs_raw_file）。

    Args:
        spark: SparkSession。
        file_path: 已废弃，保留仅为兼容；实际读取 CONFIG.hdfs_raw_file。

    Returns:
        DataFrame 或 None。
    """
    from logging_setup import get_logger

    get_logger(__name__).warning(
        "load_kaggle_ecommerce_data(file_path=) 已弃用本地路径；请使用 HDFS：%s",
        CONFIG.hdfs_raw_file,
    )
    return load_kaggle_ecommerce_data_from_hdfs(spark)


def get_spark_session() -> SparkSession:
    """复用预处理模块中的 Spark 会话构建逻辑。"""
    from preprocess import build_spark_session

    return build_spark_session("KaggleOnlineRetail")
