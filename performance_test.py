# -*- coding: utf-8 -*-
"""
分布式 vs 单机性能对比实验：Pandas 基线 vs PySpark 多分区、多数据规模。
输出：results/performance_report.md 与 results/perf_*.png
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Callable, Dict, List, Tuple

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from config import CONFIG


def _pandas_pipeline(csv_path: Path, nrows: int | None) -> float:
    """Pandas：读取 + 简化清洗 + 聚合。返回耗时（秒）。"""
    t0 = time.perf_counter()
    df = pd.read_csv(csv_path, encoding="latin-1", nrows=nrows)
    inv = df["InvoiceNo"].astype(str).str.strip()
    df = df[~inv.str.startswith("C")]
    df = df[(df["Quantity"] > 0) & (df["UnitPrice"] > 0)]
    df = df.dropna(subset=["CustomerID"])
    df["TotalAmount"] = df["Quantity"] * df["UnitPrice"]
    _ = df.groupby("InvoiceDate")["TotalAmount"].sum()
    _ = df.groupby("Country")["TotalAmount"].sum()
    _ = df.groupby("CustomerID")["TotalAmount"].sum()
    return time.perf_counter() - t0


def _spark_pipeline(csv_path: Path, fraction: float, shuffle_parts: int) -> float:
    """Spark：本地 CSV 读取 + 配置 shuffle 分区 + 简化聚合。返回耗时（秒）。"""
    from preprocess import build_spark_session
    from pyspark.sql import functions as F

    spark = None
    t0 = time.perf_counter()
    try:
        spark = build_spark_session("COMP7095PerfTest")
        spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_parts))
        df = (
            spark.read.option("header", True)
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", True)
            .option("nullValue", "NA")
            .csv(str(csv_path.resolve()))
        )
        if fraction < 1.0:
            df = df.sample(withReplacement=False, fraction=fraction, seed=42)
        df = df.filter(~F.col("InvoiceNo").cast("string").startswith("C"))
        df = df.filter((F.col("Quantity").cast("double") > 0) & (F.col("UnitPrice").cast("double") > 0))
        df = df.filter(F.col("CustomerID").isNotNull())
        df = df.withColumn("TotalAmount", F.col("Quantity").cast("double") * F.col("UnitPrice").cast("double"))
        df.groupBy("InvoiceDate").agg(F.sum("TotalAmount").alias("s")).count()
        df.groupBy("Country").agg(F.sum("TotalAmount").alias("s")).count()
        df.groupBy("CustomerID").agg(F.sum("TotalAmount").alias("s")).count()
    finally:
        if spark is not None:
            spark.stop()
    return time.perf_counter() - t0


def _ensure_results_dir() -> Path:
    p = Path.cwd() / CONFIG.local_results_dir
    p.mkdir(parents=True, exist_ok=True)
    return p


def _write_md(path: Path, lines: List[str]) -> None:
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    from logging_setup import get_logger

    log = get_logger(__name__)
    csv_path = Path(CONFIG.local_csv_path())
    if not csv_path.is_file():
        log.error("未找到数据文件：%s", csv_path)
        print(f"❌ 未找到 {csv_path}")
        return 1

    out_dir = _ensure_results_dir()
    log.info("性能测试输出目录：%s", out_dir.resolve())

    with open(csv_path, "r", encoding="latin-1", errors="replace") as f:
        total_rows = sum(1 for _ in f) - 1
    fracs = [0.25, 0.5, 0.75, 1.0]
    parts_list = [2, 4, 8, 16]

    pandas_times: Dict[float, float] = {}
    spark_grid: Dict[Tuple[float, int], float] = {}

    for frac in fracs:
        nrows = int(total_rows * frac) if frac < 1.0 else None
        try:
            pandas_times[frac] = _pandas_pipeline(csv_path, nrows)
            log.info("Pandas frac=%s 耗时 %.3fs", frac, pandas_times[frac])
        except Exception as e:
            log.exception("Pandas 测试失败 frac=%s：%s", frac, e)
            pandas_times[frac] = -1.0

    for frac in fracs:
        for sp in parts_list:
            try:
                spark_grid[(frac, sp)] = _spark_pipeline(csv_path, frac, sp)
                log.info("Spark frac=%s partitions=%s 耗时 %.3fs", frac, sp, spark_grid[(frac, sp)])
            except Exception as e:
                log.exception("Spark 测试失败 frac=%s parts=%s：%s", frac, sp, e)
                spark_grid[(frac, sp)] = -1.0

    md: List[str] = [
        "# 性能对比实验报告",
        "",
        "## 实验说明",
        "",
        "- 单机：Pandas 读取本地 CSV + 分组聚合。",
        "- 分布式：PySpark 读取本地 CSV（与课程单机环境对齐）+ 调整 `spark.sql.shuffle.partitions`。",
        f"- 数据文件：`{csv_path.name}`，估算行数：{total_rows}。",
        "",
        "## Pandas 各数据规模耗时（秒）",
        "",
        "| 数据比例 | 耗时(s) |",
        "|----------|---------|",
    ]
    for frac in fracs:
        md.append(f"| {frac*100:.0f}% | {pandas_times.get(frac, -1):.4f} |")

    md += [
        "",
        "## Spark 分区数对照（秒）",
        "",
        "| 数据比例 | " + " | ".join(f"p={p}" for p in parts_list) + " |",
        "|----------|" + "|".join(["---------" for _ in parts_list]) + "|",
    ]
    for frac in fracs:
        row = f"| {frac*100:.0f}% |"
        for sp in parts_list:
            row += f" {spark_grid.get((frac, sp), -1):.4f} |"
        md.append(row)

    report = out_dir / "performance_report.md"
    _write_md(report, md)
    log.info("已写入 %s", report)

    try:
        plt.figure(figsize=(8, 5))
        spark_100 = [spark_grid.get((1.0, p), 0) for p in parts_list]
        plt.plot(parts_list, spark_100, marker="o", label="Spark 100% 数据")
        plt.xlabel("spark.sql.shuffle.partitions")
        plt.ylabel("Time (s)")
        plt.title("Spark shuffle 分区扩展性（100% 数据）")
        plt.grid(True, alpha=0.3)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out_dir / "perf_spark_partitions.png", dpi=150)
        plt.close()

        plt.figure(figsize=(8, 5))
        x_labels = [f"{int(f*100)}%" for f in fracs]
        x = range(len(fracs))
        pd_vals = [max(0, pandas_times.get(f, 0)) for f in fracs]
        sp_vals = [max(0, spark_grid.get((f, 8), 0)) for f in fracs]
        w = 0.35
        plt.bar([i - w / 2 for i in x], pd_vals, width=w, label="Pandas")
        plt.bar([i + w / 2 for i in x], sp_vals, width=w, label="Spark (p=8)")
        plt.xticks(list(x), x_labels)
        plt.ylabel("Time (s)")
        plt.title("数据规模扩展：Pandas vs Spark")
        plt.legend()
        plt.grid(True, axis="y", alpha=0.3)
        plt.tight_layout()
        plt.savefig(out_dir / "perf_scale_compare.png", dpi=150)
        plt.close()
    except Exception as e:
        log.exception("绘图失败：%s", e)

    print(f"✅ 性能测试完成，报告：{report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
