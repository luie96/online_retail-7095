# -*- coding: utf-8 -*-
"""
模块1：Kaggle online_retail.csv 读取与校验（pandas 抽样 + PySpark 全量）
适配：Windows 路径、latin-1 编码（Kaggle 常见导出）。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import pandas as pd
from pandas.errors import EmptyDataError
from pyspark.sql import DataFrame, SparkSession

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
    功能：将 CSV 路径规范化为绝对路径（Windows 反斜杠安全）。
    输入参数：csv_path — 相对或绝对路径。
    输出结果：绝对路径 str。
    """
    p = Path(csv_path)
    if not p.is_absolute():
        p = Path.cwd() / p
    return str(p.resolve())


def validate_kaggle_data_fields(df: pd.DataFrame) -> bool:
    """
    校验 Kaggle online_retail.csv 数据集字段完整性。
    输入参数：df — pandas DataFrame（抽样或全量）。
    输出结果：字段完整返回 True，否则 False（并打印中文提示）。
    """
    missing_fields = [f for f in KAGGLE_REQUIRED_FIELDS if f not in df.columns]
    if missing_fields:
        print(f"❌ 缺失 Kaggle 数据集必填字段：{missing_fields}")
        return False
    print("✅ Kaggle online_retail.csv 字段校验通过")
    return True


def load_kaggle_ecommerce_data(
    spark: SparkSession,
    file_path: str = "./online_retail.csv",
) -> Optional[DataFrame]:
    """
    读取 Kaggle online_retail.csv 并完成基础校验。
    输入参数：
        spark — 已创建的 SparkSession；
        file_path — 数据集路径（默认当前目录 online_retail.csv）。
    输出结果：PySpark DataFrame；失败返回 None。
    """
    path = normalize_local_path(file_path)
    try:
        if not os.path.isfile(path):
            print(f"❌ 未找到文件：{path}，请确认数据集在当前目录")
            return None

        try:
            pd_sample = pd.read_csv(path, nrows=1000, encoding="latin-1")
        except EmptyDataError:
            print("❌ 数据集为空或无法解析表头")
            return None
        except Exception as e:
            print(f"❌ pandas 抽样读取失败：{e}")
            return None

        if not validate_kaggle_data_fields(pd_sample):
            return None

        if not pd.api.types.is_integer_dtype(pd_sample["Quantity"]):
            if pd_sample["Quantity"].dtype == "float64":
                s = pd_sample["Quantity"].dropna()
                if (s == s.astype("int64")).all():
                    pass
                else:
                    print("❌ Quantity 字段必须为整数类型（购买数量）")
                    return None
            else:
                print("❌ Quantity 字段必须为整数类型（购买数量）")
                return None

        if not (
            pd.api.types.is_float_dtype(pd_sample["UnitPrice"])
            or pd.api.types.is_integer_dtype(pd_sample["UnitPrice"])
        ):
            print("❌ UnitPrice 字段必须为数值类型（单价）")
            return None

        # Spark CSV 使用 Java 字符集名；ISO-8859-1 等价于 pandas 的 latin-1
        spark_df = (
            spark.read.option("header", True)
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "true")
            .option("nullValue", "NA")
            .csv(path)
        )
        n = spark_df.count()
        print(f"✅ 成功读取 Kaggle 数据集，全量数据行数：{n}")
        return spark_df

    except FileNotFoundError:
        print(f"❌ 未找到文件：{path}，请确认数据集在当前目录")
        return None
    except Exception as e:
        print(f"❌ Kaggle 数据集读取失败：{e}")
        return None


def get_spark_session() -> SparkSession:
    """复用预处理模块中的 Spark 会话构建逻辑（与 tips 命名对齐）。"""
    from preprocess import build_spark_session

    return build_spark_session("KaggleOnlineRetail")
