# -*- coding: utf-8 -*-
"""
模块3：HDFS 存储管理（Windows 下通过 subprocess 调用 hdfs.cmd，兼容 winutils 权限处理）
"""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession


def _hadoop_home() -> str:
    h = os.environ.get("HADOOP_HOME")
    if not h:
        raise EnvironmentError("未设置 HADOOP_HOME，无法定位 hdfs 与 winutils。")
    return h


def _normalize_hdfs_path(hdfs_path: str) -> str:
    """HDFS 逻辑路径统一为正斜杠。"""
    return hdfs_path.replace("\\", "/")


def _normalize_local_path(local_path: str) -> str:
    """本地路径：解析为绝对路径，供 Windows 下 subprocess 使用。"""
    p = Path(local_path)
    if not p.is_absolute():
        p = Path.cwd() / p
    return str(p.resolve())


def _hdfs_base_cmd() -> List[str]:
    """
    功能：构造 Windows 下调用 HDFS 的命令前缀。
    输出结果：例如 ['D:/app/hadoop/bin/hdfs.cmd', 'dfs'] 或 ['hdfs', 'dfs']。
    """
    home = Path(_hadoop_home())
    hdfs_cmd = home / "bin" / "hdfs.cmd"
    if hdfs_cmd.is_file():
        return [str(hdfs_cmd), "dfs"]
    return ["hdfs", "dfs"]


def _run_cmd(
    args: List[str],
    *,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """执行子进程；在 Windows 上使用默认 shell 编码，避免中文乱码。"""
    return subprocess.run(
        args,
        check=check,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _try_fix_permissions(hdfs_dir: str) -> None:
    """
    功能：在 HDFS 操作因权限失败时，依次尝试 hdfs dfs -chmod 与 winutils chmod（课程要求）。
    输入参数：hdfs_dir — HDFS 目录（如 /ecommerce）。
    """
    hp = _normalize_hdfs_path(hdfs_dir)
    base = _hdfs_base_cmd()
    try:
        _run_cmd(base + ["-chmod", "-R", "777", hp], check=True)
        return
    except subprocess.CalledProcessError:
        pass

    winutils = Path(_hadoop_home()) / "bin" / "winutils.exe"
    if winutils.is_file():
        # 部分 Windows 伪分布式场景下对本地模拟路径生效；失败则忽略
        try:
            _run_cmd([str(winutils), "chmod", "777", hp], check=False)
        except OSError:
            pass


def download_from_hdfs(hdfs_path: str, local_dir: str) -> None:
    """
    功能：将 HDFS 目录或文件递归下载到本地（hdfs dfs -get -f）。
    输入参数：hdfs_path — HDFS 源路径；local_dir — 本地目标目录（会先删除再创建）。
    输出结果：无。
    异常场景：subprocess.CalledProcessError。
    """
    hp = _normalize_hdfs_path(hdfs_path)
    ld = Path(local_dir)
    shutil.rmtree(ld, ignore_errors=True)
    ld.mkdir(parents=True, exist_ok=True)
    base = _hdfs_base_cmd()
    try:
        _run_cmd(
            base + ["-get", "-f", hp, str(ld.resolve())],
            check=True,
        )
    except subprocess.CalledProcessError:
        parent = "/".join(hp.split("/")[:-1]) or "/"
        _try_fix_permissions(parent)
        _run_cmd(
            base + ["-get", "-f", hp, str(ld.resolve())],
            check=True,
        )


def check_hdfs_file_exists(hdfs_file_path: str) -> bool:
    """
    校验 HDFS 路径是否存在（文件或目录）。

    Args:
        hdfs_file_path: HDFS 逻辑路径。

    Returns:
        存在为 True，否则 False。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    hp = _normalize_hdfs_path(hdfs_file_path)
    if not hp or hp == "/":
        log.warning("check_hdfs_file_exists：路径无效 %s", hdfs_file_path)
        return False
    base = _hdfs_base_cmd()
    try:
        cp = subprocess.run(
            base + ["-test", "-e", hp],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        ok = cp.returncode == 0
        log.debug("HDFS test -e %s => %s", hp, ok)
        return ok
    except Exception as e:
        log.error("check_hdfs_file_exists 异常：%s", e)
        return False


def init_hdfs_dir() -> None:
    """
    自动创建项目所需 HDFS 目录（已存在则跳过）。

    Raises:
        EnvironmentError: HADOOP_HOME 未设置。
        subprocess.CalledProcessError: mkdir 失败且重试仍失败。
    """
    from config import CONFIG
    from logging_setup import get_logger

    log = get_logger(__name__)
    base = _hdfs_base_cmd()
    for d in CONFIG.hdfs_subdirs_to_create():
        hp = _normalize_hdfs_path(d)
        try:
            _run_cmd(base + ["-mkdir", "-p", hp], check=True)
            log.info("HDFS 目录就绪：%s", hp)
        except subprocess.CalledProcessError as e:
            err = (e.stderr or "") + (e.stdout or "")
            if "Permission denied" in err or "permission" in err.lower():
                _try_fix_permissions("/".join(hp.split("/")[:-1]) or "/")
                _run_cmd(base + ["-mkdir", "-p", hp], check=True)
                log.info("HDFS 目录就绪（权限修复后）：%s", hp)
            else:
                log.exception("创建 HDFS 目录失败：%s", hp)
                raise


def _put_local_file_to_hdfs(local: str, target: str) -> None:
    """内部：将本地文件 put 到 HDFS target（覆盖）。"""
    target = _normalize_hdfs_path(target)
    parent = "/".join(target.split("/")[:-1]) or "/"
    base = _hdfs_base_cmd()

    def _put() -> None:
        _run_cmd(base + ["-mkdir", "-p", parent], check=False)
        _run_cmd(base + ["-put", "-f", local, target], check=True)

    try:
        _put()
    except subprocess.CalledProcessError as e:
        err = (e.stderr or "") + (e.stdout or "")
        if "Permission denied" in err or "permission" in err.lower():
            _try_fix_permissions(parent)
            _put()
        else:
            raise


def upload_local_data_to_hdfs(
    local_file_path: str,
    hdfs_target_path: str,
    *,
    upload_mode: Optional[str] = None,
) -> None:
    """
    将本地数据上传至 HDFS；支持 skip/overwrite 策略。

    Args:
        local_file_path: 本地文件绝对或相对路径。
        hdfs_target_path: HDFS 目标文件路径。
        upload_mode: 覆盖 CONFIG.hdfs_upload_mode；overwrite 或 skip。

    Raises:
        FileNotFoundError: 本地文件不存在。
        subprocess.CalledProcessError: hdfs 命令失败。
    """
    from config import CONFIG
    from logging_setup import get_logger

    log = get_logger(__name__)
    mode = (upload_mode or CONFIG.hdfs_upload_mode or "overwrite").lower().strip()
    local = _normalize_local_path(local_file_path)
    if not os.path.isfile(local):
        raise FileNotFoundError(f"本地文件不存在，无法上传：{local}")
    target = _normalize_hdfs_path(hdfs_target_path)
    if mode == "skip" and check_hdfs_file_exists(target):
        log.info("upload_mode=skip，目标已存在，跳过上传：%s", target)
        return
    log.info("上传本地文件 -> HDFS：%s => %s (mode=%s)", local, target, mode)
    try:
        _put_local_file_to_hdfs(local, target)
    except subprocess.CalledProcessError as e:
        log.error("上传 HDFS 失败：%s", e.stderr or e.stdout)
        raise


def upload_to_hdfs(local_path: str, hdfs_path: str) -> None:
    """
    功能：将本地文件上传至 HDFS；-put -f 覆盖已存在目标（等价断点策略：重复执行可覆盖）。
    输入参数：
        local_path — 本地 CSV 等文件；
        hdfs_path — 目标 HDFS 文件路径（如 /ecommerce/raw/ecommerce_user_behavior.csv）。
    输出结果：无；成功则文件在 HDFS 可用。
    异常场景：
        FileNotFoundError — 本地文件不存在；
        subprocess.CalledProcessError — hdfs 命令失败（会先尝试放宽权限后重试一次）。
    """
    upload_local_data_to_hdfs(local_path, hdfs_path, upload_mode="overwrite")


def list_hdfs_dir(hdfs_path: str) -> List[str]:
    """
    功能：列出 HDFS 目录下条目（hdfs dfs -ls 文本解析）。
    输入参数：hdfs_path — HDFS 目录。
    输出结果：行列表（原始 ls 输出行）。
    异常场景：subprocess.CalledProcessError。
    """
    hp = _normalize_hdfs_path(hdfs_path)
    base = _hdfs_base_cmd()
    try:
        cp = _run_cmd(base + ["-ls", hp], check=True)
    except subprocess.CalledProcessError as e:
        err = (e.stderr or "") + (e.stdout or "")
        if "Permission denied" in err or "permission" in err.lower():
            _try_fix_permissions(hp)
            cp = _run_cmd(base + ["-ls", hp], check=True)
        else:
            raise
    lines = [ln.strip() for ln in (cp.stdout or "").splitlines() if ln.strip()]
    return lines


def _detect_default_fs_from_cli() -> str:
    """通过 `hdfs getconf` 读取与命令行一致的 fs.defaultFS（比 Spark 内嵌 Configuration 更可靠）。"""
    env = os.environ.get("COMP7095_HDFS_DEFAULT", "").strip()
    if env.startswith("hdfs://"):
        return env.rstrip("/")
    try:
        home = Path(_hadoop_home())
        hdfs_exe = home / "bin" / "hdfs.cmd"
        exe = str(hdfs_exe) if hdfs_exe.is_file() else "hdfs"
        cp = subprocess.run(
            [exe, "getconf", "-confKey", "fs.defaultFS"],
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )
        out = (cp.stdout or "").strip()
        if cp.returncode == 0 and out.startswith("hdfs://"):
            return out.rstrip("/")
    except Exception:
        pass
    return "hdfs://localhost:9000"


def _hdfs_qualified_path(spark: SparkSession, hdfs_path: str) -> str:
    """将 /a/b 转为 hdfs://.../a/b，避免 Spark 在 local 模式下按 file:// 解析。"""
    path = _normalize_hdfs_path(hdfs_path)
    if path.startswith("hdfs://"):
        return path
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    base = hconf.get("fs.defaultFS")
    base = str(base).strip() if base else ""
    if not base.startswith("hdfs://"):
        base = _detect_default_fs_from_cli()
    return base.rstrip("/") + path


def read_from_hdfs(spark: SparkSession, hdfs_path: str, fmt: str = "parquet") -> DataFrame:
    """
    功能：从 HDFS 读取预处理结果（默认 Parquet）为 DataFrame。
    输入参数：spark — SparkSession；hdfs_path — 目录或文件；fmt — parquet 或 csv。
    输出结果：DataFrame。
    异常场景：AnalysisException；底层 IO 错误。
    """
    full = _hdfs_qualified_path(spark, hdfs_path)
    if fmt.lower() == "parquet":
        return spark.read.parquet(full)
    if fmt.lower() == "csv":
        return spark.read.option("header", True).csv(full)
    raise ValueError(f"不支持的格式：{fmt}")


def _write_parquet_fallback_local_put(df: DataFrame, hdfs_dir: str) -> None:
    """
    当 Spark 直接写 Parquet 在 Windows 上因 Hadoop NativeIO JNI 失败时的回退路径：
    Spark 写本地单分区 Parquet → hdfs dfs -put -f 上传到目标目录（仍满足「Parquet 在 HDFS」）。

    说明：
    - 严禁将 Spark DataFrame 转为 Pandas 进行单机计算回退；这里保持全程 Spark 写出。
    - 该回退仅在 Windows NativeIO JNI 兼容性问题时触发，用于保障可复现性。
    """
    hdfs_dir = _normalize_hdfs_path(hdfs_dir).rstrip("/")
    base = _hdfs_base_cmd()
    local_root = Path.cwd() / "tmp" / "parquet_hdfs_fallback"
    shutil.rmtree(local_root, ignore_errors=True)
    local_root.mkdir(parents=True, exist_ok=True)
    local_uri = local_root.resolve().as_uri()

    # 使用 Spark 生成单文件 Parquet（本地目录结构内为 part-*.parquet）
    df.coalesce(1).write.mode("overwrite").parquet(local_uri)
    parts = sorted(local_root.glob("part-*.parquet"))
    if not parts:
        shutil.rmtree(local_root, ignore_errors=True)
        raise RuntimeError(f"Parquet 回退写本地失败：未生成 part 文件：{local_root}")
    local_file = parts[0]

    _run_cmd(base + ["-rm", "-r", "-f", hdfs_dir], check=False)
    _run_cmd(base + ["-mkdir", "-p", hdfs_dir], check=True)
    remote_file = hdfs_dir + "/part-00000.parquet"
    try:
        _run_cmd(base + ["-put", "-f", str(local_file.resolve()), remote_file], check=True)
    except subprocess.CalledProcessError:
        parent = "/".join(hdfs_dir.split("/")[:-1]) or "/"
        _try_fix_permissions(parent)
        _run_cmd(base + ["-put", "-f", str(local_file.resolve()), remote_file], check=True)
    shutil.rmtree(local_root, ignore_errors=True)


def _is_nativeio_link_failure(exc: BaseException) -> bool:
    s = str(exc)
    return "UnsatisfiedLinkError" in s or "NativeIO" in s or "createDirectoryWithMode0" in s


def write_parquet_to_hdfs(df: DataFrame, hdfs_path: str) -> None:
    """
    功能：将 DataFrame 以 Parquet 列式格式写入 HDFS（overwrite）。
    输入参数：df — 预处理结果；hdfs_path — 目标目录（如 /ecommerce/processed/cleaned）。
    输出结果：无。
    异常场景：写入失败时尝试 chmod 后重试；仍失败则抛出异常。
    """
    path = _normalize_hdfs_path(hdfs_path)
    parent = "/".join(path.rstrip("/").split("/")[:-1]) or "/"

    hconf = df.sparkSession.sparkContext._jsc.hadoopConfiguration()
    hconf.setBoolean("io.native.lib.available", False)

    def _write() -> None:
        qualified = _hdfs_qualified_path(df.sparkSession, path)
        df.write.mode("overwrite").parquet(qualified)

    try:
        _write()
    except Py4JJavaError as e:
        if _is_nativeio_link_failure(e):
            _write_parquet_fallback_local_put(df, path)
            return
        msg = str(e).lower()
        if "permission" in msg or "access" in msg:
            _try_fix_permissions(parent)
            _write()
        else:
            raise
    except Exception as e:
        if _is_nativeio_link_failure(e):
            _write_parquet_fallback_local_put(df, path)
            return
        msg = str(e).lower()
        if "permission" in msg or "access" in msg:
            _try_fix_permissions(parent)
            _write()
        else:
            raise


def read_data_from_hdfs(
    hdfs_file_path: str,
    spark_session: SparkSession,
    *,
    fmt: str = "csv",
) -> DataFrame:
    """
    从 HDFS 读取数据为 Spark DataFrame（与 PySpark 规范一致，路径自动 qualify）。

    Args:
        hdfs_file_path: HDFS 文件或目录路径。
        spark_session: SparkSession。
        fmt: csv 或 parquet。

    Returns:
        DataFrame。

    Raises:
        ValueError: 不支持的格式。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    fmt_l = (fmt or "csv").lower().strip()
    log.info("从 HDFS 读取：%s 格式=%s", hdfs_file_path, fmt_l)
    if fmt_l == "csv":
        full = _hdfs_qualified_path(spark_session, hdfs_file_path)
        return (
            spark_session.read.option("header", True)
            .option("encoding", "ISO-8859-1")
            .option("inferSchema", "true")
            .option("nullValue", "NA")
            .csv(full)
        )
    if fmt_l == "parquet":
        return read_from_hdfs(spark_session, hdfs_file_path, fmt="parquet")
    raise ValueError(f"read_data_from_hdfs 不支持格式：{fmt}")


def write_result_to_hdfs(
    df: DataFrame,
    hdfs_output_path: str,
    write_mode: str = "overwrite",
    *,
    file_format: str = "csv",
) -> None:
    """
    将分析结果写入 HDFS，支持 parquet 或 csv（csv 为 Spark 多 part 目录结构）。

    Args:
        df: 结果 DataFrame。
        hdfs_output_path: HDFS 输出目录路径。
        write_mode: Spark 写入模式，如 overwrite、append。
        file_format: parquet 或 csv。

    Raises:
        ValueError: 不支持的 file_format。
    """
    from logging_setup import get_logger

    log = get_logger(__name__)
    path = _normalize_hdfs_path(hdfs_output_path).rstrip("/")
    fmt = (file_format or "csv").lower().strip()
    qualified = _hdfs_qualified_path(df.sparkSession, path)
    hconf = df.sparkSession.sparkContext._jsc.hadoopConfiguration()
    hconf.setBoolean("io.native.lib.available", False)
    log.info("写入 HDFS 结果：%s format=%s mode=%s", qualified, fmt, write_mode)
    try:
        if fmt == "parquet":
            df.write.mode(write_mode).parquet(qualified)
        elif fmt == "csv":
            df.write.mode(write_mode).option("header", True).option("encoding", "UTF-8").csv(qualified)
        else:
            raise ValueError(f"write_result_to_hdfs 不支持格式：{file_format}")
    except Py4JJavaError as e:
        msg = str(e).lower()
        parent = "/".join(path.split("/")[:-1]) or "/"
        if "permission" in msg or "access" in msg:
            _try_fix_permissions(parent)
            if fmt == "parquet":
                df.write.mode(write_mode).parquet(qualified)
            else:
                df.write.mode(write_mode).option("header", True).option("encoding", "UTF-8").csv(qualified)
        else:
            raise


def close_hdfs_client() -> None:
    """
    安全关闭 HDFS 客户端。当前实现基于 hdfs 子进程，无长连接，仅记录日志。
    """
    from logging_setup import get_logger

    get_logger(__name__).info("HDFS 客户端已关闭（subprocess 模式无持久连接）。")
