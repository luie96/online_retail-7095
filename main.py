# -*- coding: utf-8 -*-
"""
模块5：全流程调度入口 — Kaggle online_retail.csv（Windows + HDFS + PySpark）
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
from config import CONFIG


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
    from logging_setup import get_logger

    log = get_logger(__name__)
    jh = os.environ.get("JAVA_HOME", "")
    hh = os.environ.get("HADOOP_HOME", "")
    log.info("JAVA_HOME=%s", jh or "(未设置)")
    log.info("HADOOP_HOME=%s", hh or "(未设置)")
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


def _check_kaggle_local_file() -> None:
    """前置：确认本地 Kaggle 数据文件存在（用于上传 HDFS）。"""
    p = Path(CONFIG.local_csv_path())
    if not p.is_file():
        raise FileNotFoundError(
            f"未找到本地 {p.name}，请将数据集放在项目根目录，或通过 COMP7095_LOCAL_CSV 指定文件名"
        )


def main() -> int:
    from logging_setup import setup_logging, get_logger

    setup_logging()
    log = get_logger(__name__)
    err_path = _reset_error_log()
    init_windows_environment()
    _log_env()

    if "--perf" in sys.argv or CONFIG.run_performance_test:
        log.info("进入性能测试模式")
        try:
            import performance_test

            return performance_test.main()
        except Exception as e:
            _write_error_log(err_path, e)
            log.exception("性能测试失败：%s", e)
            print(f"\n❌ 性能测试失败，详情见 error.log：{e}", file=sys.stderr)
            return 1

    spark = None
    try:
        _check_kaggle_local_file()

        print("=" * 50)
        print("【步骤 1/6】初始化 HDFS 目录")
        print("=" * 50)
        hdfs_manager.init_hdfs_dir()

        print("\n" + "=" * 50)
        print("【步骤 2/6】上传本地 CSV 至 HDFS")
        print("=" * 50)
        hdfs_manager.upload_local_data_to_hdfs(
            CONFIG.local_csv_path(),
            CONFIG.hdfs_raw_file,
        )

        print("\n" + "=" * 50)
        print("【步骤 3/6】从 HDFS 读取数据并预处理")
        print("=" * 50)
        spark = preprocess.build_spark_session(CONFIG.spark_app_name)
        spark_df = data_loader.load_kaggle_ecommerce_data_from_hdfs(spark)
        if spark_df is None:
            raise RuntimeError("从 HDFS 读取 Kaggle 数据集失败，终止流程")
        df_cleaned = preprocess.preprocess_kaggle_data(spark_df)

        print("\n" + "=" * 50)
        print("【步骤 4/6】预处理结果写入 HDFS（Parquet）")
        print("=" * 50)
        hdfs_manager.write_parquet_to_hdfs(df_cleaned, CONFIG.hdfs_processed_dir)
        print("       ✅ 预处理 Parquet 已写入 HDFS。")

        print("\n" + "=" * 50)
        print("【步骤 5/6】从 HDFS 回读清洗数据并执行分布式分析")
        print("=" * 50)
        hdfs_manager.list_hdfs_dir(CONFIG.hdfs_processed_dir)
        df_for_analysis = hdfs_manager.read_data_from_hdfs(
            CONFIG.hdfs_processed_dir,
            spark,
            fmt="parquet",
        )
        results_dir = str(Path.cwd() / CONFIG.local_results_dir)
        distributed_processing.process_kaggle_retail_data(df_for_analysis, results_dir)

        print("\n" + "=" * 50)
        print("【步骤 6/6】校验结果文件")
        print("=" * 50)
        if not distributed_processing.verify_results_written():
            raise RuntimeError("results 目录下部分结果 CSV 缺失或为空。")

        print("\n" + "=" * 50)
        print("✅ Kaggle online_retail.csv 全流程执行完成！")
        print(f"📊 结果文件路径：{os.path.abspath(results_dir)}")
        print("=" * 50)
        return 0

    except Exception as e:
        _write_error_log(err_path, e)
        log.exception("流程执行失败：%s", e)
        print(f"\n❌ 流程执行失败，详情见 error.log：{e}", file=sys.stderr)
        return 1

    finally:
        try:
            hdfs_manager.close_hdfs_client()
        except Exception:
            pass
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass
        _cleanup_local_dirs()


if __name__ == "__main__":
    sys.exit(main())
