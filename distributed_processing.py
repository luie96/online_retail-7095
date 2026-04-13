# -*- coding: utf-8 -*-
"""
模块4：Kaggle online_retail 零售指标（PySpark 聚合 + 单文件 CSV 写出，适配 Windows）
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _ensure_output_dir(output_dir: str) -> Path:
    p = Path(output_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_single_csv_from_spark(df: DataFrame, filepath: str) -> None:
    """
    将聚合后的 Spark DataFrame 写成单个 UTF-8 CSV（避免 Spark write.csv 产生 part 目录及 Windows NativeIO 问题）。
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf = df.toPandas()
    pdf.to_csv(filepath, index=False, encoding="utf-8")


def process_kaggle_retail_data(
    df_cleaned: DataFrame,
    output_dir: str = "./results",
) -> bool:
    """
    Kaggle online_retail 零售数据分布式计算，输出 5 个指标 CSV。
    输入参数：
        df_cleaned — 预处理后的 PySpark DataFrame；
        output_dir — 结果目录，默认 ./results。
    输出结果：成功返回 True。
    """
    out = _ensure_output_dir(output_dir)

    # 1) 每日订单与销售
    daily_sales = (
        df_cleaned.groupBy("InvoiceDate")
        .agg(
            F.countDistinct("InvoiceNo").alias("DailyOrderCount"),
            F.sum("TotalAmount").alias("DailySalesAmount"),
            F.sum("Quantity").alias("DailyTotalQuantity"),
        )
        .orderBy("InvoiceDate")
    )
    _write_single_csv_from_spark(daily_sales, str(out / "daily_sales.csv"))
    print("✅ 指标1：每日销售统计已输出")

    # 2) 商品销量 TOP20
    top20_products = (
        df_cleaned.groupBy("StockCode", "Description")
        .agg(
            F.sum("Quantity").alias("TotalSalesQuantity"),
            F.sum("TotalAmount").alias("TotalSalesAmount"),
        )
        .orderBy(F.desc("TotalSalesQuantity"))
        .limit(20)
    )
    _write_single_csv_from_spark(top20_products, str(out / "top20_products.csv"))
    print("✅ 指标2：商品销量 TOP20 已输出")

    # 3) 国家/地区销售分布
    country_sales = (
        df_cleaned.groupBy("Country")
        .agg(
            F.countDistinct("InvoiceNo").alias("OrderCount"),
            F.sum("TotalAmount").alias("SalesAmount"),
            F.countDistinct("CustomerID").alias("CustomerCount"),
        )
        .orderBy(F.desc("SalesAmount"))
    )
    _write_single_csv_from_spark(country_sales, str(out / "country_sales.csv"))
    print("✅ 指标3：地区销售分布已输出")

    # 4) 客户购买频次
    customer_frequency = (
        df_cleaned.groupBy("CustomerID")
        .agg(
            F.countDistinct("InvoiceNo").alias("PurchaseFrequency"),
            F.sum("TotalAmount").alias("TotalSpending"),
            F.avg("TotalAmount").alias("AvgOrderSpending"),
        )
        .orderBy(F.desc("TotalSpending"))
    )
    _write_single_csv_from_spark(
        customer_frequency, str(out / "customer_frequency.csv")
    )
    print("✅ 指标4：客户购买频次统计已输出")

    # 5) 小时订单分布
    hourly_orders = (
        df_cleaned.groupBy("InvoiceHour")
        .agg(
            F.countDistinct("InvoiceNo").alias("HourlyOrderCount"),
            F.sum("TotalAmount").alias("HourlySalesAmount"),
        )
        .orderBy(F.col("InvoiceHour").cast("int"))
    )
    _write_single_csv_from_spark(hourly_orders, str(out / "hourly_orders.csv"))
    print("✅ 指标5：小时订单分布已输出")

    return True


def expected_result_files() -> list:
    return [
        "daily_sales.csv",
        "top20_products.csv",
        "country_sales.csv",
        "customer_frequency.csv",
        "hourly_orders.csv",
    ]


def verify_results_written() -> bool:
    rd = Path.cwd() / "results"
    for name in expected_result_files():
        p = rd / name
        if not p.is_file() or p.stat().st_size <= 0:
            return False
    return True
