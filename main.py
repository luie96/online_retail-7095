# -*- coding: utf-8 -*-
"""
模块5：全流程调度入口 — Kaggle online_retail.csv（Windows + HDFS）
"""

from __future__ import annotations

import io
import os
import shutil
import sys
import traceback
from pathlib import Path

import data_loader
import distributed_processing
import hdfs_manager
import preprocess

HDFS_RAW_FILE = "/ecommerce/raw/kaggle_online_retail.csv"
HDFS_PROCESSED_DIR = "/ecommerce/processed/kaggle_cleaned_data"
KAGGLE_CSV = "online_retail.csv"


def init_windows_environment() -> None:
    """初始化 Windows 运行环境（Hadoop JVM 参数、UTF-8 控制台、临时目录）。"""
    flag = "-Dio.native.lib.available=false"
    for key in ("HADOOP_OPTS", "SPARK_HADOOP_OPTS"):
        cur = os.environ.get(key, "").strip()
        if flag not in cur:
            os.environ[key] = (cur + " " + flag).strip() if cur else flag
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except (AttributeError, OSError):
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
        sys.stderr = io.TextIOWrapper(
            sys.stderr.buffer, encoding="utf-8", errors="replace", line_buffering=True
        )
    for name in ("spark-warehouse", "tmp"):
        (Path.cwd() / name).mkdir(parents=True, exist_ok=True)


def _log_env() -> None:
    jh = os.environ.get("JAVA_HOME", "")
    hh = os.environ.get("HADOOP_HOME", "")
    print(f"[环境] JAVA_HOME={jh or '(未设置)'}")
    print(f"[环境] HADOOP_HOME={hh or '(未设置)'}")


def _cleanup_local_dirs() -> None:
    for name in ("spark-warehouse", "tmp"):
        p = Path.cwd() / name
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)


def _reset_error_log() -> Path:
    p = Path.cwd() / "error.log"
    p.write_text("", encoding="utf-8")
    return p


def _write_error_log(err_path: Path, exc: BaseException) -> None:
    err_path.write_text(
        "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
        encoding="utf-8",
    )


def _check_kaggle_file() -> None:
    """前置：确认 Kaggle 数据文件存在。"""
    p = Path.cwd() / KAGGLE_CSV
    if not p.is_file():
        raise FileNotFoundError(
            f"未找到 {KAGGLE_CSV}，请将 Kaggle 数据集放在项目根目录，文件名严格为 online_retail.csv"
        )


def main() -> int:
    err_path = _reset_error_log()
    init_windows_environment()
    _log_env()

    spark = None
    try:
        _check_kaggle_file()

        print("=" * 50)
        print("【步骤 1/5】读取 Kaggle online_retail.csv 数据集")
        print("=" * 50)
        spark = preprocess.build_spark_session("COMP7095KaggleRetail")
        spark_df = data_loader.load_kaggle_ecommerce_data(
            spark, file_path=KAGGLE_CSV
        )
        if spark_df is None:
            raise RuntimeError("Kaggle 数据集读取失败，终止流程")

        print("\n" + "=" * 50)
        print("【步骤 2/5】预处理 Kaggle online_retail.csv 数据集")
        print("=" * 50)
        df_cleaned = preprocess.preprocess_kaggle_data(spark_df)

        print("\n" + "=" * 50)
        print("【步骤 3/5】存储数据到 HDFS（Kaggle 零售数据）")
        print("=" * 50)
        local_csv = data_loader.normalize_local_path(KAGGLE_CSV)
        hdfs_manager.upload_to_hdfs(local_csv, HDFS_RAW_FILE)
        print("       ✅ 原始 CSV 已上传 HDFS。")
        hdfs_manager.write_parquet_to_hdfs(df_cleaned, HDFS_PROCESSED_DIR)
        print("       ✅ 预处理 Parquet 已写入 HDFS。")

        print("\n" + "=" * 50)
        print("【步骤 4/5】Kaggle 零售数据分布式计算")
        print("=" * 50)
        hdfs_manager.list_hdfs_dir(HDFS_PROCESSED_DIR)
        distributed_processing.process_kaggle_retail_data(df_cleaned, "./results")

        print("\n" + "=" * 50)
        print("【步骤 5/5】校验结果文件")
        print("=" * 50)
        if not distributed_processing.verify_results_written():
            raise RuntimeError("results 目录下 5 个 Kaggle 结果 CSV 缺失或为空。")

        print("\n" + "=" * 50)
        print("✅ Kaggle online_retail.csv 全流程执行完成！")
        print(f"📊 结果文件路径：{os.path.abspath('./results')}")
        print("=" * 50)
        return 0

    except Exception as e:
        _write_error_log(err_path, e)
        print(f"\n❌ 流程执行失败，详情见 error.log：{e}", file=sys.stderr)
        return 1

    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        _cleanup_local_dirs()


if __name__ == "__main__":
    sys.exit(main())
