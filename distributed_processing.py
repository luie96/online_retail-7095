# -*- coding: utf-8 -*-
"""
模块4：Kaggle online_retail 零售指标（PySpark 全分布式，无 Pandas 计算回退）
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import CONFIG


def _ensure_output_dir(output_dir: str) -> Path:
    p = Path(output_dir)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_single_csv_spark_local(df: DataFrame, filepath: str) -> None:
    """
    将 Spark DataFrame 写成单个本地 UTF-8 CSV（coalesce(1) + 移动 part 文件）。

    Args:
        df: 聚合后的 Spark DataFrame。
        filepath: 目标 .csv 路径。

    Raises:
        RuntimeError: 未生成 part 文件。
        OSError: 磁盘错误。
    """
    """
    Windows 下 Spark 写本地 CSV 可能触发 Hadoop NativeIO JNI 错误；
    为保证可复现性，这里优先通过 HDFS 临时目录导出并 getmerge 回本地单文件。
    """
    import hdfs_manager
    from logging_setup import get_logger

    log = get_logger(__name__)
    out = Path(filepath)
    out.parent.mkdir(parents=True, exist_ok=True)

    tmp_hdfs = f"{CONFIG.hdfs_results_dir.rstrip('/')}/_tmp_local_export/{out.stem}"
    tmp_hdfs = tmp_hdfs.replace(" ", "_")
    try:
        # 1) 写到 HDFS 临时目录（CSV 目录结构）
        hdfs_manager.write_result_to_hdfs(
            df.coalesce(1),
            tmp_hdfs,
            write_mode="overwrite",
            file_format="csv",
        )
        # 2) 合并下载到本地单文件
        hdfs_manager.getmerge_hdfs_dir_to_local(tmp_hdfs, str(out.resolve()))
    finally:
        try:
            hdfs_manager.remove_hdfs_path(tmp_hdfs)
        except Exception as e:
            log.warning("清理 HDFS 临时目录失败：%s", e)


def _hdfs_result_subdir(base_name: str) -> str:
    """HDFS 上结果子目录（不含扩展名）。"""
    safe = base_name.replace(".csv", "").replace(" ", "_")
    return f"{CONFIG.hdfs_results_dir.rstrip('/')}/{safe}"


def _save_and_push(
    df: DataFrame,
    base_name: str,
    output_dir: str,
    *,
    log_label: str,
) -> None:
    """写入本地单文件 CSV，并写入 HDFS（CSV 目录）。"""
    import hdfs_manager
    from logging_setup import get_logger

    log = get_logger(__name__)
    out = _ensure_output_dir(output_dir)
    local_path = str(out / f"{base_name}.csv")
    _write_single_csv_spark_local(df, local_path)
    hdfs_manager.write_result_to_hdfs(
        df,
        _hdfs_result_subdir(base_name),
        write_mode="overwrite",
        file_format="csv",
    )
    log.info("%s 已写入本地与 HDFS：%s", log_label, local_path)


def _timed(name: str, fn, *args, **kwargs):
    from logging_setup import get_logger

    log = get_logger(__name__)
    t0 = time.perf_counter()
    try:
        return fn(*args, **kwargs)
    finally:
        log.info("⏱ %s 耗时 %.3f 秒", name, time.perf_counter() - t0)


def _repartition_for_compute(df: DataFrame) -> DataFrame:
    n = max(1, int(CONFIG.spark_repartition_n))
    return df.repartition(n, F.col("CustomerID"))


def analyze_daily_sales(df: DataFrame) -> DataFrame:
    """每日订单与销售。"""
    return (
        df.groupBy("InvoiceDate")
        .agg(
            F.countDistinct("InvoiceNo").alias("DailyOrderCount"),
            F.sum("TotalAmount").alias("DailySalesAmount"),
            F.sum("Quantity").alias("DailyTotalQuantity"),
        )
        .orderBy("InvoiceDate")
    )


def analyze_top20_products(df: DataFrame) -> DataFrame:
    """商品销量 TOP20。"""
    return (
        df.groupBy("StockCode", "Description")
        .agg(
            F.sum("Quantity").alias("TotalSalesQuantity"),
            F.sum("TotalAmount").alias("TotalSalesAmount"),
        )
        .orderBy(F.desc("TotalSalesQuantity"))
        .limit(20)
    )


def analyze_country_sales(df: DataFrame) -> DataFrame:
    """国家/地区销售分布。"""
    return (
        df.groupBy("Country")
        .agg(
            F.countDistinct("InvoiceNo").alias("OrderCount"),
            F.sum("TotalAmount").alias("SalesAmount"),
            F.countDistinct("CustomerID").alias("CustomerCount"),
        )
        .orderBy(F.desc("SalesAmount"))
    )


def analyze_customer_frequency(df: DataFrame) -> DataFrame:
    """客户购买频次。"""
    return (
        df.groupBy("CustomerID")
        .agg(
            F.countDistinct("InvoiceNo").alias("PurchaseFrequency"),
            F.sum("TotalAmount").alias("TotalSpending"),
            F.avg("TotalAmount").alias("AvgOrderSpending"),
        )
        .orderBy(F.desc("TotalSpending"))
    )


def analyze_hourly_orders(df: DataFrame) -> DataFrame:
    """小时订单分布。"""
    return (
        df.groupBy("InvoiceHour")
        .agg(
            F.countDistinct("InvoiceNo").alias("HourlyOrderCount"),
            F.sum("TotalAmount").alias("HourlySalesAmount"),
        )
        .orderBy(F.col("InvoiceHour").cast("int"))
    )


def analyze_rfm_segments(df: DataFrame) -> DataFrame:
    """
    RFM 用户价值：Recency / Frequency / Monetary 与分层。
    Recency：相对全局最大日期的天数差（越小越好）。
    """
    ref_row = df.select(F.max("InvoiceDateTime").alias("ref")).collect()[0]
    ref = ref_row["ref"]
    ref_lit = F.lit(ref)
    per_c = df.groupBy("CustomerID").agg(
        F.min(F.datediff(ref_lit, F.col("InvoiceDateTime"))).alias("RecencyDays"),
        F.countDistinct("InvoiceNo").alias("Frequency"),
        F.sum("TotalAmount").alias("Monetary"),
    )
    wr = Window.orderBy(F.col("RecencyDays").asc())
    wf = Window.orderBy(F.col("Frequency").desc())
    wm = Window.orderBy(F.col("Monetary").desc())
    scored = (
        per_c.withColumn("R_q", F.ntile(4).over(wr))
        .withColumn("F_q", F.ntile(4).over(wf))
        .withColumn("M_q", F.ntile(4).over(wm))
    )
    seg = (
        F.when((F.col("R_q") <= 2) & (F.col("F_q") <= 2) & (F.col("M_q") <= 2), F.lit("重要价值用户"))
        .when((F.col("R_q") <= 2) & (F.col("F_q") <= 2), F.lit("重要发展用户"))
        .when((F.col("R_q") >= 3) & (F.col("F_q") <= 2), F.lit("重要挽留用户"))
        .when((F.col("F_q") >= 3) & (F.col("M_q") <= 2), F.lit("一般用户"))
        .otherwise(F.lit("流失风险用户"))
    )
    return scored.withColumn("RFM_Segment", seg).select(
        "CustomerID", "RecencyDays", "Frequency", "Monetary", "R_q", "F_q", "M_q", "RFM_Segment"
    )


def analyze_association_rules(df: DataFrame) -> DataFrame:
    """FP-Growth 购物篮关联规则（支持度、置信度、提升度）。"""
    baskets = (
        df.groupBy("InvoiceNo")
        .agg(F.array_sort(F.collect_set(F.col("StockCode").cast("string"))).alias("items"))
        .filter(F.size(F.col("items")) >= 2)
    )
    fp = FPGrowth(
        itemsCol="items",
        minSupport=float(CONFIG.fpgrowth_min_support),
        minConfidence=float(CONFIG.fpgrowth_min_confidence),
    )
    model = fp.fit(baskets)
    rules = model.associationRules
    return rules.select(
        F.array_join(F.col("antecedent"), ",").alias("antecedent_str"),
        F.array_join(F.col("consequent"), ",").alias("consequent_str"),
        F.col("support"),
        F.col("confidence"),
        F.col("lift"),
    ).orderBy(F.desc("lift"))


def analyze_user_behavior_summary(df: DataFrame) -> DataFrame:
    """用户行为摘要：复购率、客单价分位数（订单级）、回购周期均值等。"""
    per_c = df.groupBy("CustomerID").agg(F.countDistinct("InvoiceNo").alias("n_orders"))
    rep = per_c.agg(
        F.count(F.lit(1)).alias("customer_cnt"),
        F.sum(F.when(F.col("n_orders") >= 2, 1).otherwise(0)).alias("repeat_customers"),
    )
    rep = rep.withColumn(
        "repurchase_rate",
        F.col("repeat_customers").cast("double") / F.col("customer_cnt").cast("double"),
    )
    order_amt = df.groupBy("InvoiceNo").agg(F.sum("TotalAmount").alias("order_value"))
    qv = order_amt.approxQuantile("order_value", [0.25, 0.5,0.75], 0.05)
    # 避免使用 createDataFrame(list) 触发 Python worker 通信问题：用现有 DataFrame 构造单行常量结果
    q25 = order_amt.limit(1).select(
        F.lit("order_value_p25").alias("metric"), F.lit(float(qv[0])).alias("value")
    )
    q50 = order_amt.limit(1).select(
        F.lit("order_value_p50").alias("metric"), F.lit(float(qv[1])).alias("value")
    )
    q75 = order_amt.limit(1).select(
        F.lit("order_value_p75").alias("metric"), F.lit(float(qv[2])).alias("value")
    )
    qrow = q25.unionByName(q50).unionByName(q75)
    w = Window.partitionBy("CustomerID").orderBy("InvoiceDateTime")
    gaps = (
        df.withColumn("prev_ts", F.lag("InvoiceDateTime").over(w))
        .filter(F.col("prev_ts").isNotNull())
        .withColumn("gap_days", F.datediff(F.col("InvoiceDateTime"), F.col("prev_ts")))
    )
    avg_gap = (
        gaps.agg(F.avg("gap_days").alias("value"))
        .withColumn("metric", F.lit("avg_repurchase_gap_days"))
        .select("metric", "value")
    )
    rep_long = rep.select(
        F.lit("customer_cnt").alias("metric"),
        F.col("customer_cnt").cast("double").alias("value"),
    ).unionByName(
        rep.select(
            F.lit("repeat_customers").alias("metric"),
            F.col("repeat_customers").cast("double").alias("value"),
        )
    ).unionByName(
        rep.select(F.lit("repurchase_rate").alias("metric"), F.col("repurchase_rate").alias("value"))
    )
    return rep_long.unionByName(qrow).unionByName(avg_gap)


def analyze_first_purchase_distribution(df: DataFrame) -> DataFrame:
    """首次消费时间分布（按首次购买月份聚合客户数）。"""
    w = Window.partitionBy("CustomerID").orderBy(F.col("InvoiceDateTime").asc())
    first_ts = (
        df.withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .withColumn("first_month", F.date_format("InvoiceDateTime", "yyyy-MM"))
    )
    return first_ts.groupBy("first_month").agg(F.countDistinct("CustomerID").alias("NewCustomers"))


def analyze_monthly_sales_trend(df: DataFrame) -> DataFrame:
    """月度销售额及环比。"""
    monthly = (
        df.withColumn("ym", F.date_format("InvoiceDateTime", "yyyy-MM"))
        .groupBy("ym")
        .agg(F.sum("TotalAmount").alias("sales"))
        .orderBy("ym")
    )
    win = Window.orderBy("ym")
    return (
        monthly.withColumn("prev_sales", F.lag("sales").over(win))
        .withColumn(
            "mom_pct",
            F.when(F.col("prev_sales").isNull() | (F.col("prev_sales") == 0), F.lit(None)).otherwise(
                (F.col("sales") - F.col("prev_sales")) / F.col("prev_sales") * F.lit(100.0)
            ),
        )
        .select("ym", "sales", "mom_pct")
    )


def analyze_product_seasonality(df: DataFrame) -> DataFrame:
    """商品销售季节性：按年内月份聚合。"""
    return (
        df.withColumn("month_of_year", F.month("InvoiceDateTime"))
        .groupBy("month_of_year")
        .agg(F.sum("TotalAmount").alias("MonthlySalesAmount"), F.sum("Quantity").alias("MonthlyQuantity"))
        .orderBy("month_of_year")
    )


def analyze_country_sales_growth(df: DataFrame) -> DataFrame:
    """国家/地区年度销售额及同比。"""
    cy = (
        df.withColumn("yr", F.year("InvoiceDateTime"))
        .groupBy("Country", "yr")
        .agg(F.sum("TotalAmount").alias("sales"))
    )
    win = Window.partitionBy("Country").orderBy("yr")
    return (
        cy.withColumn("prev_sales", F.lag("sales").over(win))
        .withColumn(
            "yoy_pct",
            F.when(F.col("prev_sales").isNull() | (F.col("prev_sales") == 0), F.lit(None)).otherwise(
                (F.col("sales") - F.col("prev_sales")) / F.col("prev_sales") * F.lit(100.0)
            ),
        )
        .orderBy("Country", "yr")
    )


def process_kaggle_retail_data(
    df_cleaned: DataFrame,
    output_dir: str = "./results",
) -> bool:
    """
    Kaggle online_retail 全部分析：经典 5 指标 + RFM、关联规则、用户行为、销售深度。

    Args:
        df_cleaned: 预处理后的 DataFrame（建议自 HDFS 回读）。
        output_dir: 本地结果目录。

    Returns:
        成功返回 True。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    dfw = _repartition_for_compute(df_cleaned)
    log.info("分布式分析开始，repartition=%s", CONFIG.spark_repartition_n)

    jobs = [
        ("每日销售", "daily_sales", analyze_daily_sales),
        ("TOP20 商品", "top20_products", analyze_top20_products),
        ("国家销售", "country_sales", analyze_country_sales),
        ("客户频次", "customer_frequency", analyze_customer_frequency),
        ("小时订单", "hourly_orders", analyze_hourly_orders),
        ("RFM 分层", "rfm_segments", analyze_rfm_segments),
        ("关联规则", "association_rules", analyze_association_rules),
        ("用户行为摘要", "user_behavior_summary", analyze_user_behavior_summary),
        ("首购分布", "first_purchase_distribution", analyze_first_purchase_distribution),
        ("月度趋势", "monthly_sales_trend", analyze_monthly_sales_trend),
        ("销售季节性", "product_seasonality", analyze_product_seasonality),
        ("国家同比增长", "country_sales_growth", analyze_country_sales_growth),
    ]

    for title, fname, fn in jobs:
        def _run():
            out_df = fn(dfw)
            _save_and_push(out_df, fname, output_dir, log_label=title)
            print(f"✅ {title} 已输出（本地 + HDFS）")

        _timed(title, _run)

    return True


def expected_result_files() -> list:
    return [
        "daily_sales.csv",
        "top20_products.csv",
        "country_sales.csv",
        "customer_frequency.csv",
        "hourly_orders.csv",
        "rfm_segments.csv",
        "association_rules.csv",
        "user_behavior_summary.csv",
        "first_purchase_distribution.csv",
        "monthly_sales_trend.csv",
        "product_seasonality.csv",
        "country_sales_growth.csv",
    ]


def verify_results_written() -> bool:
    from logging_setup import get_logger

    log = get_logger(__name__)
    rd = Path.cwd() / CONFIG.local_results_dir
    for name in expected_result_files():
        p = rd / name
        if not p.is_file() or p.stat().st_size <= 0:
            log.error("结果缺失或为空：%s", p)
            return False
    return True
