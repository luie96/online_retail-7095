# -*- coding: utf-8 -*-
"""
全局配置：路径、HDFS、Spark、分析阈值等均从此模块读取，业务代码禁止硬编码。
可通过环境变量覆盖（前缀 COMP7095_），例如 COMP7095_HDFS_ROOT。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import List


def _env_str(name: str, default: str) -> str:
    v = os.environ.get(f"COMP7095_{name}", "").strip()
    return v if v else default


def _env_int(name: str, default: int) -> int:
    v = os.environ.get(f"COMP7095_{name}", "").strip()
    if not v:
        return default
    try:
        return int(v)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(f"COMP7095_{name}", "").strip().lower()
    if v in ("1", "true", "yes", "y"):
        return True
    if v in ("0", "false", "no", "n"):
        return False
    return default


def _env_float(name: str, default: float) -> float:
    v = os.environ.get(f"COMP7095_{name}", "").strip()
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


@dataclass
class AppConfig:
    """应用级配置，供 main / 各模块导入 `CONFIG` 使用。"""

    # --- 本地路径 ---
    local_csv_filename: str = field(default_factory=lambda: _env_str("LOCAL_CSV", "online_retail.csv"))
    local_results_dir: str = field(default_factory=lambda: _env_str("RESULTS_DIR", "results"))
    local_logs_dir: str = field(default_factory=lambda: _env_str("LOGS_DIR", "logs"))

    # --- HDFS 逻辑路径 ---
    hdfs_root: str = field(default_factory=lambda: _env_str("HDFS_ROOT", "/ecommerce"))
    hdfs_raw_file: str = field(
        default_factory=lambda: _env_str("HDFS_RAW", "/ecommerce/raw/kaggle_online_retail.csv")
    )
    hdfs_processed_dir: str = field(
        default_factory=lambda: _env_str("HDFS_PROCESSED", "/ecommerce/processed/kaggle_cleaned_data")
    )
    hdfs_results_dir: str = field(
        default_factory=lambda: _env_str("HDFS_RESULTS", "/ecommerce/results/analysis_outputs")
    )

    # 上传策略：overwrite | skip（目标已存在则跳过）
    hdfs_upload_mode: str = field(default_factory=lambda: _env_str("HDFS_UPLOAD_MODE", "overwrite"))

    # --- Spark（local 与集群均可通过环境覆盖）---
    spark_app_name: str = field(default_factory=lambda: _env_str("SPARK_APP", "COMP7095KaggleRetail"))
    spark_master: str = field(default_factory=lambda: _env_str("SPARK_MASTER", "local[*]"))
    spark_default_parallelism: int = field(default_factory=lambda: _env_int("SPARK_DEFAULT_PARALLELISM", 8))
    spark_sql_shuffle_partitions: int = field(
        default_factory=lambda: _env_int("SPARK_SHUFFLE_PARTITIONS", 16)
    )
    spark_executor_memory: str = field(default_factory=lambda: _env_str("SPARK_EXECUTOR_MEMORY", "2g"))
    spark_executor_cores: int = field(default_factory=lambda: _env_int("SPARK_EXECUTOR_CORES", 4))
    spark_driver_memory: str = field(default_factory=lambda: _env_str("SPARK_DRIVER_MEMORY", "2g"))
    spark_sql_timezone: str = field(default_factory=lambda: _env_str("SPARK_TIMEZONE", "Europe/London"))

    # 大数据集 repartition 目标分区数
    spark_repartition_n: int = field(default_factory=lambda: _env_int("SPARK_REPARTITION_N", 16))

    # --- 分析阈值 ---
    fpgrowth_min_support: float = field(default_factory=lambda: _env_float("FP_MIN_SUPPORT", 0.01))
    fpgrowth_min_confidence: float = field(default_factory=lambda: _env_float("FP_MIN_CONFIDENCE", 0.2))
    rfm_recency_anchor_days: int = field(default_factory=lambda: _env_int("RFM_ANCHOR_DAYS", 0))

    # --- 运行开关 ---
    run_performance_test: bool = field(default_factory=lambda: _env_bool("RUN_PERF_TEST", False))

    def local_csv_path(self) -> str:
        from pathlib import Path

        return str((Path.cwd() / self.local_csv_filename).resolve())

    def hdfs_subdirs_to_create(self) -> List[str]:
        r = self.hdfs_root.rstrip("/")
        return [
            r,
            f"{r}/raw",
            f"{r}/processed",
            f"{r}/results",
            self.hdfs_processed_dir.rstrip("/"),
            self.hdfs_results_dir.rstrip("/"),
        ]


CONFIG = AppConfig()
