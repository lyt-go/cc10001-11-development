"""分片输出业务规则：按管道规则去重、映射、封片，生成可交付结果清单。

设计约定：

* 去重键相同只保留「最新」一条。有可解析的更新时间时按时间取最新，时间相同或缺失时
  按采集顺序取后出现的记录，保证重复执行结果确定。
* 分片同时受记录数阈值和未压缩字节阈值约束，任意一个达到阈值就封片；一条记录不会被
  切开，单条记录超过大小阈值时单独成片。
* 同一任务 + 批次的文件都落在 ``<task>/<batch>/``，重复执行会覆盖同一份清单并清理旧分片，
  不会产生重复分片。
* 每写完一个分片就原子更新结果清单和采集日志，所以刷新页面时清单里的记录数始终等于
  已写入分片的记录数之和。
* 分片写入失败时保留已经完成的分片、在清单里写明失败原因，并记录失败位置；恢复时复用
  已落盘的规范化数据计划，跳过已完成记录，从失败分片继续，不重新打包全部数据。
"""
from __future__ import annotations

import codecs
import csv
import gzip
import hashlib
import io
import json
import os
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator

from app.store import store

# 输出文件默认放在 backend/data/exports；测试或部署可以通过环境变量改到临时目录。
DEFAULT_EXPORT_ROOT = Path(__file__).resolve().parents[2] / "data" / "exports"
EXPORT_ROOT = Path(os.environ.get("WCH_EXPORT_DIR") or DEFAULT_EXPORT_ROOT)

MANIFEST_NAME = "manifest.json"
PLAN_NAME = "_plan.json.gz"

STATUS_RUNNING = "执行中"
STATUS_SUCCESS = "成功"
STATUS_PARTIAL = "部分成功"
STATUS_FAILED = "失败"

DEFAULT_BATCH = "default"
META_KEYS = {"id", "status", "pending", "abnormal", "关联任务", "关联批次"}

_LOCKS_GUARD = threading.Lock()
_LOCKS: dict[tuple[str, str, str], threading.Lock] = {}

_CODE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_CN_NUMERAL = "零一二三四五六七八九十百千万亿"
_RECORD_THRESHOLD_RE = re.compile(
    rf"(?:(\d+(?:\.\d+)?)\s*(万|亿)?|([{_CN_NUMERAL}]+))\s*(?:条|行|records?|rows?)",
    re.IGNORECASE,
)
_SIZE_THRESHOLD_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(B|KB|MB|GB)", re.IGNORECASE)
_CN_UNITS = {"十": 10, "百": 100, "千": 1000, "万": 10000, "亿": 100000000}
_CN_DIGITS = {"零": 0, "一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9}


class ExportBadRequest(Exception):
    """请求参数或业务配置不合法。"""


class ExportNotFound(Exception):
    """任务、管道、配置、批次或分片不存在。"""


@dataclass
class ExportSettings:
    """一次输出实际生效的配置快照。"""

    config_code: str
    config_version: str
    output_format: str = "ndjson"
    compression: str = "none"
    encoding: str = "utf-8"
    delimiter: str = ","
    max_records: int | None = None
    max_bytes: int | None = None
    target_path: str = ""
    snapshot: dict[str, Any] = field(default_factory=dict)

    def manifest_dict(self) -> dict[str, Any]:
        return {
            "code": self.config_code,
            "version": self.config_version,
            "output_format": self.output_format,
            "compression": self.compression,
            "encoding": self.encoding,
            "delimiter": self.delimiter,
            "max_records": self.max_records,
            "max_bytes": self.max_bytes,
            "target_path": self.target_path,
            "snapshot": self.snapshot,
        }

    @classmethod
    def from_manifest(cls, payload: dict[str, Any]) -> "ExportSettings":
        return cls(
            config_code=str(payload.get("code") or ""),
            config_version=str(payload.get("version") or ""),
            output_format=str(payload.get("output_format") or "ndjson"),
            compression=str(payload.get("compression") or "none"),
            encoding=str(payload.get("encoding") or "utf-8"),
            delimiter=str(payload.get("delimiter") if payload.get("delimiter") is not None else ","),
            max_records=payload.get("max_records"),
            max_bytes=payload.get("max_bytes"),
            target_path=str(payload.get("target_path") or ""),
            snapshot=dict(payload.get("snapshot") or {}),
        )


@dataclass
class PipelineInfo:
    code: str
    version: str
    dedupe_fields: list[str]
    field_mapping: dict[str, str]
    invalid_policy: str
    snapshot: dict[str, Any] = field(default_factory=dict)

    def manifest_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "version": self.version,
            "dedupe_field": "、".join(self.dedupe_fields),
            "dedupe_fields": list(self.dedupe_fields),
            "field_mapping": dict(self.field_mapping),
            "invalid_policy": self.invalid_policy,
            "snapshot": self.snapshot,
        }

    @classmethod
    def from_manifest(cls, payload: dict[str, Any]) -> "PipelineInfo":
        fields = payload.get("dedupe_fields")
        if not fields:
            raw = str(payload.get("dedupe_field") or "")
            fields = [item.strip() for item in re.split(r"[、,，]", raw) if item.strip()]
        return cls(
            code=str(payload.get("code") or ""),
            version=str(payload.get("version") or ""),
            dedupe_fields=list(fields or []),
            field_mapping=dict(payload.get("field_mapping") or {}),
            invalid_policy=str(payload.get("invalid_policy") or "跳过"),
            snapshot=dict(payload.get("snapshot") or {}),
        )


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "是"}


def _safe_component(value: str, label: str) -> str:
    """任务编号、批次编号会拼进文件路径，必须先验证，避免路径穿越。"""
    value = value.strip()
    if not value:
        raise ExportBadRequest(f"缺少必填字段：{label}")
    if not _CODE_RE.match(value):
        raise ExportBadRequest(f"{label}「{value}」不合法，只允许字母、数字、下划线、点和短横线，且不超过 64 个字符")
    return value


def parse_field_mapping(raw: Any) -> dict[str, str]:
    """解析字段映射。

    支持 JSON 对象（源字段 -> 输出字段）、JSON 数组或逗号/顿号分隔的输出字段名（同名映射），
    以及 ``源字段:输出字段`` 形式的简写。
    """
    if isinstance(raw, dict):
        return {str(key): str(value) for key, value in raw.items()}
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        decoded = None
    if decoded is not None:
        return parse_field_mapping(decoded)
    result: dict[str, str] = {}
    for part in re.split(r"[、,，;；\n]+", text):
        part = part.strip()
        if not part:
            continue
        matched = re.split(r"[:：=→]", part, maxsplit=1)
        if len(matched) == 2 and matched[0].strip() and matched[1].strip():
            result[matched[0].strip()] = matched[1].strip()
        else:
            result[part] = part
    return result


def parse_dedupe_fields(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(item).strip() for item in raw if str(item).strip()]
    return [item.strip() for item in re.split(r"[、,，;；]+", str(raw or "")) if item.strip()]


def _cn_number(text: str) -> int | None:
    """把「五万」「一万五千」「十二万」这类阈值写法转成整数。"""
    if not text:
        return None
    total = 0
    section = 0
    number = 0
    for char in text:
        if char in _CN_DIGITS:
            number = _CN_DIGITS[char]
        elif char in _CN_UNITS:
            unit = _CN_UNITS[char]
            if unit >= 10000:
                section = (section + number) * unit
                total += section
                section = 0
            else:
                if number == 0:
                    number = 1
                section += number * unit
            number = 0
        else:
            return None
    return total + section + number


def _parse_shard_thresholds(raw: Any) -> tuple[int | None, int | None]:
    """解析分片阈值，例如 ``50000条/64MB``、``五万条或64MB``、``64MB``、``50000``。"""
    if isinstance(raw, dict):
        max_records = raw.get("max_records") or raw.get("记录数")
        max_bytes = raw.get("max_bytes") or raw.get("大小")
        return (
            int(max_records) if max_records else None,
            int(max_bytes) if max_bytes else None,
        )
    text = str(raw or "").strip()
    if not text:
        return None, None

    max_records: int | None = None
    record_match = _RECORD_THRESHOLD_RE.search(text)
    if record_match:
        if record_match.group(1):
            amount = float(record_match.group(1))
            if record_match.group(2):
                amount *= 10000 if record_match.group(2) == "万" else 100000000
            max_records = int(amount)
        else:
            max_records = _cn_number(record_match.group(3) or "")
    elif not _SIZE_THRESHOLD_RE.search(text):
        # 没有单位时按记录数理解（例如 "50000" 或 "五万"）
        plain = re.search(r"\d+(?:\.\d+)?", text)
        cn_plain = re.search(rf"[{_CN_NUMERAL}]+", text)
        if plain:
            max_records = int(float(plain.group(0)))
        elif cn_plain:
            max_records = _cn_number(cn_plain.group(0))

    max_bytes: int | None = None
    size_match = _SIZE_THRESHOLD_RE.search(text)
    if size_match:
        amount = float(size_match.group(1))
        unit = size_match.group(2).upper()
        multiplier = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3}[unit]
        max_bytes = int(amount * multiplier)

    if max_records is not None and max_records <= 0:
        max_records = None
    if max_bytes is not None and max_bytes <= 0:
        max_bytes = None
    return max_records, max_bytes


def normalize_output_format(raw: Any, default: str = "ndjson") -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return default
    if "ndjson" in text or "jsonl" in text or "json lines" in text or "jsonlines" in text:
        return "ndjson"
    if text in {"json", "json array", "json数组"} or "json" == text:
        return "json"
    if "csv" in text:
        return "csv"
    if "tsv" in text:
        return "tsv"
    if "xlsx" in text or "excel" in text or "parquet" in text:
        raise ExportBadRequest(f"输出格式「{raw}」暂不支持，可用格式：ndjson、json、csv、tsv")
    return default


def normalize_compression(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return "none"
    if text in {"gzip", "gz", "gzip压缩", "压缩"} or "gzip" in text:
        return "gzip"
    if text in {"none", "无", "不压缩", "uncompressed", "原始"}:
        return "none"
    if "zip" in text or "snappy" in text or "zstd" in text:
        raise ExportBadRequest(f"压缩算法「{raw}」暂不支持，可用：gzip 或不压缩")
    return "none"


def _normalize_encoding(raw: Any) -> str:
    text = str(raw or "").strip() or "utf-8"
    try:
        codecs.lookup(text)
    except LookupError as exc:
        raise ExportBadRequest(f"字符编码「{raw}」不可用") from exc
    return text


def _parse_timestamp(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return _number_timestamp(float(value))
    text = str(value).strip()
    if not text:
        return None
    try:
        return _number_timestamp(float(text))
    except ValueError:
        pass
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        parsed = None
    if parsed is None:
        for pattern in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _number_timestamp(value: float) -> float:
    # 毫秒时间戳折算成秒，保持不同来源可比较。
    return value / 1000.0 if value > 1_000_000_000_000 else value


def _json_dump(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


_UPDATED_AT_CANDIDATES = ("更新时间", "更新时间戳", "updated_at", "update_time", "updatedAt", "时间")


def _record_timestamp(raw: dict[str, Any], data: dict[str, Any], field_mapping: dict[str, str]) -> float | None:
    for field_name in _UPDATED_AT_CANDIDATES:
        if field_name in raw:
            parsed = _parse_timestamp(raw.get(field_name))
            if parsed is not None:
                return parsed
    for source, destination in field_mapping.items():
        if str(destination) in {"更新时间", "updated_at", "update_time"}:
            parsed = _parse_timestamp(raw.get(source))
            if parsed is not None:
                return parsed
    for field_name in _UPDATED_AT_CANDIDATES:
        if field_name in data:
            parsed = _parse_timestamp(data.get(field_name))
            if parsed is not None:
                return parsed
    return None


def _dedupe_key(raw: dict[str, Any], data: dict[str, Any], fields: list[str]) -> tuple[str, str] | None:
    parts: list[str] = []
    for field_name in fields:
        value = raw.get(field_name)
        if value is None and field_name in data:
            value = data.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        parts.append(str(value).strip())
    if not parts:
        return None
    return "\x1f".join(parts), "|".join(parts)


def _normalize_record(raw: dict[str, Any], field_mapping: dict[str, str]) -> dict[str, Any]:
    if not field_mapping:
        return {key: value for key, value in raw.items() if key not in META_KEYS}
    return {destination: raw.get(source) for source, destination in field_mapping.items()}


@dataclass
class PlanItem:
    key: str
    display_key: str
    data: dict[str, Any]


class ExportRunService:
    def __init__(self, root: Path | str | None = None) -> None:
        self.root = Path(root) if root is not None else EXPORT_ROOT
        self.root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ 对外查询

    def list_manifests(
        self,
        *,
        task_code: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        items: list[dict[str, Any]] = []
        for manifest_path in sorted(self.root.glob("*/*/manifest.json")):
            try:
                manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if task_code and str(manifest.get("task_code")) != task_code:
                continue
            items.append(self._manifest_summary(manifest))
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        total = len(items)
        start = max(page - 1, 0) * size
        return items[start:start + size], total

    def get_manifest(self, task_code: str, batch: str, *, reconcile: bool = True) -> dict[str, Any] | None:
        manifest = self._load_manifest(task_code, batch)
        if manifest is None:
            return None
        if reconcile:
            self._reconcile(manifest)
        return manifest

    def shard_file(self, task_code: str, batch: str, index: int) -> Path | None:
        manifest = self._load_manifest(task_code, batch)
        if manifest is None:
            return None
        for shard in manifest.get("shards", []):
            if int(shard.get("index", 0)) == index:
                path = self._batch_dir(task_code, batch) / str(shard.get("file") or "")
                if path.is_file():
                    return path
        return None

    # ------------------------------------------------------------------ 执行入口

    def execute(self, values: dict[str, Any]) -> dict[str, Any]:
        task_code = _safe_component(str(values.get("task_code") or ""), "任务编号")
        batch = str(values.get("batch") or DEFAULT_BATCH).strip() or DEFAULT_BATCH
        batch = _safe_component(batch, "批次编号")

        resume = _as_bool(values.get("resume"))
        records_given = values.get("records") is not None
        records = values.get("records")
        if records_given and not isinstance(records, list):
            raise ExportBadRequest("records 必须是记录数组")

        lock = self._lock_for(task_code, batch)
        with lock:
            if resume:
                return self._execute_resume(
                    task_code=task_code,
                    batch=batch,
                    records_given=records_given,
                    records=records,
                    simulate_failure_at_shard=values.get("simulate_failure_at_shard"),
                )
            return self._execute_fresh(
                task_code=task_code,
                batch=batch,
                pipeline_code=str(values.get("pipeline_code") or "").strip(),
                export_code=str(values.get("export_code") or "").strip(),
                records_given=records_given,
                records=records,
                simulate_failure_at_shard=values.get("simulate_failure_at_shard"),
            )

    def _lock_for(self, task_code: str, batch: str) -> threading.Lock:
        """同一任务同一批次串行执行；并发请求排队后覆盖同一批次，最终结果一致。"""
        key = (str(self.root.resolve()), task_code, batch)
        with _LOCKS_GUARD:
            lock = _LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                _LOCKS[key] = lock
            return lock

    # ------------------------------------------------------------------ 新执行

    def _execute_fresh(
        self,
        *,
        task_code: str,
        batch: str,
        pipeline_code: str,
        export_code: str,
        records_given: bool,
        records: Any,
        simulate_failure_at_shard: Any,
    ) -> dict[str, Any]:
        if not pipeline_code:
            raise ExportBadRequest("缺少必填字段：管道编号")
        if not export_code:
            raise ExportBadRequest("缺少必填字段：输出配置编号")
        pipeline_row = self._find_row("pipeline", "管道编号", pipeline_code)
        if pipeline_row is None:
            raise ExportNotFound(f"管道 {pipeline_code} 不存在或已归档")
        export_row = self._find_row("exportconf", "配置编号", export_code)
        if export_row is None:
            raise ExportNotFound(f"输出配置 {export_code} 不存在或已停用")
        if self._find_row("taskcrawl", "任务编号", task_code) is None:
            raise ExportNotFound(f"采集任务 {task_code} 不存在或已归档")

        pipeline = self._build_pipeline_info(pipeline_row)
        settings = self._build_settings(export_row, pipeline_row)

        raw_records = records if records_given else self._load_source_records(task_code)
        plan_items, stats = self._build_plan(raw_records, pipeline)

        batch_dir = self._batch_dir(task_code, batch)
        batch_dir.mkdir(parents=True, exist_ok=True)
        self._clean_batch(batch_dir)

        fingerprint = self._fingerprint(pipeline, settings)
        plan_digest = self._plan_digest(fingerprint, plan_items)
        self._write_plan(batch_dir, plan_items)

        manifest: dict[str, Any] = {
            "task_code": task_code,
            "batch": batch,
            "pipeline": pipeline.manifest_dict(),
            "output_config": settings.manifest_dict(),
            "fingerprint": fingerprint,
            "plan": {"file": PLAN_NAME, "digest": plan_digest, "record_count": len(plan_items)},
            "status": STATUS_RUNNING,
            "failure_reason": None,
            "failed_shard_index": None,
            "input_count": stats["input_count"],
            "invalid_count": stats["invalid_count"],
            "deduplicated_count": stats["deduplicated_count"],
            "planned_record_count": len(plan_items),
            "record_count": 0,
            "shard_count": 0,
            "shards": [],
            "attempts": [],
            "started_at": _now(),
            "updated_at": _now(),
            "finished_at": None,
        }
        self._save_manifest(task_code, batch, manifest)
        return self._run_shards(
            task_code=task_code,
            batch=batch,
            manifest=manifest,
            settings=settings,
            plan_items=iter(plan_items),
            start_index=1,
            written_before=0,
            simulate_failure_at_shard=simulate_failure_at_shard,
            resumed_from_shard=None,
        )

    # ------------------------------------------------------------------ 恢复执行

    def _execute_resume(
        self,
        *,
        task_code: str,
        batch: str,
        records_given: bool,
        records: Any,
        simulate_failure_at_shard: Any,
    ) -> dict[str, Any]:
        manifest = self._load_manifest(task_code, batch)
        if manifest is None:
            raise ExportNotFound(f"任务 {task_code} 批次 {batch} 没有可恢复的结果清单")
        if manifest.get("status") == STATUS_SUCCESS:
            self._reconcile(manifest)
            return manifest

        plan_meta = manifest.get("plan") or {}
        plan_path = self._batch_dir(task_code, batch) / str(plan_meta.get("file") or PLAN_NAME)
        if not plan_path.is_file():
            raise ExportNotFound(f"批次 {batch} 的规范化数据计划已丢失，无法从失败位置继续，请重新执行")

        pipeline = PipelineInfo.from_manifest(manifest.get("pipeline") or {})
        settings = ExportSettings.from_manifest(manifest.get("output_config") or {})

        completed = 0
        for shard in manifest.get("shards", []):
            if str(shard.get("status")) == "已完成":
                completed += int(shard.get("record_count", 0))
        start_index = int(manifest.get("failed_shard_index") or (len(manifest.get("shards", [])) + 1))

        plan_items = list(self._read_plan(plan_path))
        if records_given:
            rebuilt, _stats = self._build_plan(records, pipeline)
            if self._plan_digest(str(manifest.get("fingerprint") or ""), rebuilt) != str(plan_meta.get("digest") or ""):
                raise ExportBadRequest("输入数据已变化，不能从失败位置继续；请以 resume=false 重新执行该批次")
            plan_items = rebuilt

        if completed > len(plan_items):
            raise ExportBadRequest("结果清单记录的已完成条数超过规范化数据总量，请重新执行该批次")

        manifest["status"] = STATUS_RUNNING
        manifest["failure_reason"] = None
        manifest["failed_shard_index"] = None
        manifest["updated_at"] = _now()
        manifest.setdefault("attempts", [])
        self._save_manifest(task_code, batch, manifest)

        return self._run_shards(
            task_code=task_code,
            batch=batch,
            manifest=manifest,
            settings=settings,
            plan_items=iter(plan_items[completed:]),
            start_index=start_index,
            written_before=completed,
            simulate_failure_at_shard=simulate_failure_at_shard,
            resumed_from_shard=start_index,
        )

    # ------------------------------------------------------------------ 封片与写入

    def _run_shards(
        self,
        *,
        task_code: str,
        batch: str,
        manifest: dict[str, Any],
        settings: ExportSettings,
        plan_items: Iterator[PlanItem],
        start_index: int,
        written_before: int,
        simulate_failure_at_shard: Any,
        resumed_from_shard: int | None,
    ) -> dict[str, Any]:
        batch_dir = self._batch_dir(task_code, batch)
        started = _now()
        attempt = {
            "attempt": len(manifest.get("attempts", [])) + 1,
            "started_at": started,
            "finished_at": None,
            "status": STATUS_RUNNING,
            "resumed_from_shard": resumed_from_shard,
            "failure_reason": None,
        }
        manifest.setdefault("attempts", []).append(attempt)

        fail_at = self._parse_fail_at(simulate_failure_at_shard)
        field_order = pipeline_field_order(manifest.get("pipeline") or {})
        plan_items, first_item = _peek(plan_items)
        if not field_order and first_item is not None:
            field_order = [str(key) for key in first_item.data.keys()]
        serializer = _ShardSerializer(settings, field_order)
        written = written_before
        indexes = start_index

        try:
            for shard_items, shard_bytes in self._group_shards(plan_items, serializer, settings):
                if fail_at is not None and indexes == fail_at:
                    raise OSError("演练：分片写入失败")
                index = indexes
                shard = self._write_shard(batch_dir, index, shard_items, shard_bytes, settings, serializer)
                shard["index"] = index
                shard["status"] = "已完成"
                manifest["shards"].append(shard)
                written += int(shard["record_count"])
                manifest["record_count"] = written
                manifest["shard_count"] = len(manifest["shards"])
                manifest["planned_record_count"] = int(
                    (manifest.get("plan") or {}).get("record_count") or manifest.get("planned_record_count") or 0
                )
                manifest["status"] = STATUS_RUNNING
                manifest["updated_at"] = _now()
                self._save_manifest(task_code, batch, manifest)
                self._sync_log(task_code, batch, manifest, attempt)
                indexes += 1
        except Exception as exc:  # 分片写入失败要落盘成可读原因，而不是只返回一次请求错误
            manifest["failed_shard_index"] = indexes
            reason = f"分片 {indexes} 写入失败：{exc}"
            manifest["failure_reason"] = reason
            manifest["status"] = STATUS_PARTIAL if written else STATUS_FAILED
            manifest["record_count"] = written
            manifest["shard_count"] = len(manifest["shards"])
            manifest["updated_at"] = _now()
            manifest["finished_at"] = _now()
            attempt["finished_at"] = manifest["finished_at"]
            attempt["status"] = manifest["status"]
            attempt["failure_reason"] = reason
            self._save_manifest(task_code, batch, manifest)
            self._sync_log(task_code, batch, manifest, attempt)
            return manifest

        planned = int((manifest.get("plan") or {}).get("record_count") or 0)
        invalid = int(manifest.get("invalid_count") or 0)
        if written != planned:
            # 正常情况下不会发生；留作防御，避免清单和分片条数悄悄不一致。
            manifest["failure_reason"] = f"写入条数 {written} 与计划条数 {planned} 不一致"
            manifest["status"] = STATUS_PARTIAL if written else STATUS_FAILED
        elif invalid:
            manifest["status"] = STATUS_PARTIAL
            manifest["failure_reason"] = f"{invalid} 条记录因缺少去重键被跳过"
        elif planned == 0 and int(manifest.get("input_count") or 0) > 0:
            manifest["status"] = STATUS_FAILED
            manifest["failure_reason"] = "输入记录均无法生成有效去重键"
        else:
            manifest["status"] = STATUS_SUCCESS
            manifest["failure_reason"] = None
        manifest["record_count"] = written
        manifest["shard_count"] = len(manifest["shards"])
        manifest["finished_at"] = _now()
        manifest["updated_at"] = manifest["finished_at"]
        manifest.setdefault("attempts", [])[-1]["finished_at"] = manifest["finished_at"]
        manifest["attempts"][-1]["status"] = manifest["status"]
        manifest["attempts"][-1]["failure_reason"] = manifest["failure_reason"]
        self._save_manifest(task_code, batch, manifest)
        self._sync_log(task_code, batch, manifest, None)
        return manifest

    def _group_shards(
        self,
        items: Iterator[PlanItem],
        serializer: "_ShardSerializer",
        settings: ExportSettings,
    ) -> Iterator[tuple[list[PlanItem], int]]:
        """按记录数和字节阈值把计划记录分组；两端阈值都包含边界。"""
        current: list[PlanItem] = []
        content_bytes = 0
        for item in items:
            payload_size = len(serializer.item_bytes(item.data))
            next_count = len(current) + 1
            next_size = serializer.size_with(next_count, content_bytes + payload_size)
            over_count = settings.max_records is not None and next_count > settings.max_records
            over_size = settings.max_bytes is not None and next_size > settings.max_bytes
            if current and (over_count or over_size):
                yield current, serializer.size_with(len(current), content_bytes)
                current = []
                content_bytes = 0
            current.append(item)
            content_bytes += payload_size
        if current:
            yield current, serializer.size_with(len(current), content_bytes)

    def _write_shard(
        self,
        batch_dir: Path,
        index: int,
        items: list[PlanItem],
        size_bytes: int,
        settings: ExportSettings,
        serializer: "_ShardSerializer",
    ) -> dict[str, Any]:
        file_name = f"part-{index:05d}{_format_extension(settings)}"
        temp_path = batch_dir / f".{file_name}.{uuid.uuid4().hex}.tmp"
        final_path = batch_dir / file_name
        first_key = items[0].display_key if items else ""
        last_key = items[-1].display_key if items else ""
        try:
            with open(temp_path, "wb") as raw_file:
                if settings.compression == "gzip":
                    # mtime=0 让相同输入生成可复现的压缩字节。
                    with gzip.GzipFile(fileobj=raw_file, mode="wb", filename="", mtime=0) as target:
                        self._write_shard_content(target, items, settings, serializer)
                else:
                    self._write_shard_content(raw_file, items, settings, serializer)
            actual_size = temp_path.stat().st_size
            os.replace(temp_path, final_path)
        except Exception:
            if temp_path.exists():
                temp_path.unlink(missing_ok=True)
            raise
        return {
            "file": file_name,
            "record_count": len(items),
            "size_bytes": actual_size,
            "uncompressed_bytes": size_bytes,
            "first_key": first_key,
            "last_key": last_key,
            "compressed": settings.compression == "gzip",
        }

    def _write_shard_content(
        self,
        target: Any,
        items: list[PlanItem],
        settings: ExportSettings,
        serializer: "_ShardSerializer",
    ) -> None:
        fmt = settings.output_format
        if fmt == "json":
            target.write(b"[")
            for position, item in enumerate(items):
                if position:
                    target.write(b",")
                target.write(serializer.item_bytes(item.data))
            target.write(b"]\n")
            return
        if fmt in {"csv", "tsv"}:
            target.write(serializer.header_bytes())
        for item in items:
            target.write(serializer.item_bytes(item.data))

    # ------------------------------------------------------------------ 计划构建

    def _build_plan(
        self,
        raw_records: Iterable[Any],
        pipeline: PipelineInfo,
    ) -> tuple[list[PlanItem], dict[str, int]]:
        if not isinstance(raw_records, list):
            raw_records = list(raw_records)
        input_count = len(raw_records)
        invalid_count = 0
        deduplicated_count = 0
        chosen: dict[str, tuple[float, int, str, dict[str, Any]]] = {}

        for position, raw in enumerate(raw_records):
            if not isinstance(raw, dict):
                invalid_count += 1
                continue
            data = _normalize_record(raw, pipeline.field_mapping)
            key_pair = _dedupe_key(raw, data, pipeline.dedupe_fields)
            if key_pair is None:
                invalid_count += 1
                continue
            internal_key, display_key = key_pair
            timestamp = _record_timestamp(raw, data, pipeline.field_mapping)
            previous = chosen.get(internal_key)
            if previous is None:
                chosen[internal_key] = (timestamp if timestamp is not None else float("-inf"), position, display_key, data)
            else:
                deduplicated_count += 1
                prev_ts, _prev_pos, _prev_display, _prev_data = previous
                new_ts = timestamp if timestamp is not None else float("-inf")
                # 时间相同时后出现的记录胜出，保证顺序稳定且可解释。
                if new_ts >= prev_ts:
                    chosen[internal_key] = (new_ts, position, display_key, data)

        ordered_plan = [
            PlanItem(key=internal, display_key=item[2], data=item[3])
            for internal, item in sorted(chosen.items(), key=lambda pair: pair[1][1])
        ]
        return ordered_plan, {
            "input_count": input_count,
            "invalid_count": invalid_count,
            "deduplicated_count": deduplicated_count,
        }

    def _load_source_records(self, task_code: str) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        with store.lock:
            for row in store.rows("collectresult"):
                if str(row.get("关联任务") or "") == task_code:
                    records.append(dict(row))
        return records
        records: list[dict[str, Any]] = []
        with store.lock:
            for row in store.rows("collectresult"):
                if str(row.get("关联任务") or "") == task_code:
                    records.append(dict(row))
        return records

    # ------------------------------------------------------------------ 配置解析

    def _find_row(self, module: str, field_name: str, code: str) -> dict[str, Any] | None:
        with store.lock:
            for row in store.rows(module):
                if str(row.get(field_name) or "").strip() == code:
                    return dict(row)
        return None

    def _build_pipeline_info(self, row: dict[str, Any]) -> PipelineInfo:
        mapping = parse_field_mapping(row.get("字段映射"))
        dedupe_fields = parse_dedupe_fields(row.get("去重字段"))
        if not dedupe_fields:
            raise ExportBadRequest("管道未配置去重字段，无法生成去重后的结果清单")
        version = str(row.get("管道版本") or row.get("版本") or f"v{row.get('id')}")
        policy = str(row.get("异常行策略") or "跳过")
        return PipelineInfo(
            code=str(row.get("管道编号") or ""),
            version=version,
            dedupe_fields=dedupe_fields,
            field_mapping=mapping,
            invalid_policy=policy,
            snapshot={key: str(value) for key, value in row.items() if key not in META_KEYS},
        )

    def _build_settings(self, export_row: dict[str, Any], pipeline_row: dict[str, Any]) -> ExportSettings:
        output_format = normalize_output_format(
            export_row.get("输出格式") or pipeline_row.get("输出格式") or "ndjson"
        )
        compression = normalize_compression(export_row.get("压缩算法"))
        encoding = _normalize_encoding(export_row.get("字符编码") or "UTF-8")
        delimiter = str(export_row.get("列分隔符") or ",")
        max_records, max_bytes = _parse_shard_thresholds(export_row.get("分片阈值"))
        version = str(export_row.get("配置版本") or export_row.get("版本") or f"v{export_row.get('id')}")
        return ExportSettings(
            config_code=str(export_row.get("配置编号") or ""),
            config_version=version,
            output_format=output_format,
            compression=compression,
            encoding=encoding,
            delimiter=delimiter,
            max_records=max_records,
            max_bytes=max_bytes,
            target_path=str(export_row.get("目标路径") or ""),
            snapshot={key: str(value) for key, value in export_row.items() if key not in META_KEYS},
        )

    # ------------------------------------------------------------------ 清单与日志

    def _fingerprint(self, pipeline: PipelineInfo, settings: ExportSettings) -> str:
        payload = {
            "pipeline": {
                "dedupe": pipeline.dedupe_fields,
                "mapping": pipeline.field_mapping,
                "version": pipeline.version,
            },
            "output": {
                "format": settings.output_format,
                "compression": settings.compression,
                "encoding": settings.encoding,
                "delimiter": settings.delimiter,
                "max_records": settings.max_records,
                "max_bytes": settings.max_bytes,
                "version": settings.config_version,
            },
        }
        return hashlib.sha256(_json_dump(payload).encode("utf-8")).hexdigest()

    def _plan_digest(self, fingerprint: str, items: list[PlanItem]) -> str:
        digest = hashlib.sha256(fingerprint.encode("utf-8"))
        for item in items:
            digest.update(_json_dump({"key": item.key, "data": item.data}).encode("utf-8"))
            digest.update(b"\n")
        return digest.hexdigest()

    def _write_plan(self, batch_dir: Path, items: list[PlanItem]) -> None:
        temp_path = batch_dir / f".{PLAN_NAME}.{uuid.uuid4().hex}.tmp"
        try:
            with open(temp_path, "wb") as raw_file:
                with gzip.GzipFile(fileobj=raw_file, mode="wb", filename="", mtime=0) as gz:
                    for item in items:
                        gz.write((_json_dump({"key": item.key, "display_key": item.display_key, "data": item.data}) + "\n").encode("utf-8"))
            os.replace(temp_path, batch_dir / PLAN_NAME)
        except Exception:
            temp_path.unlink(missing_ok=True)
            raise

    def _read_plan(self, plan_path: Path) -> Iterator[PlanItem]:
        with gzip.open(plan_path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                payload = json.loads(line)
                yield PlanItem(
                    key=str(payload.get("key") or ""),
                    display_key=str(payload.get("display_key") or payload.get("key") or ""),
                    data=dict(payload.get("data") or {}),
                )

    def _clean_batch(self, batch_dir: Path) -> None:
        for path in batch_dir.glob("part-*"):
            if path.is_file():
                path.unlink(missing_ok=True)
        for name in (MANIFEST_NAME, PLAN_NAME):
            target = batch_dir / name
            if target.exists():
                target.unlink(missing_ok=True)

    def _batch_dir(self, task_code: str, batch: str) -> Path:
        _safe_component(task_code, "任务编号")
        _safe_component(batch, "批次编号")
        return self.root / task_code / batch

    def _manifest_path(self, task_code: str, batch: str) -> Path:
        return self._batch_dir(task_code, batch) / MANIFEST_NAME

    def _load_manifest(self, task_code: str, batch: str) -> dict[str, Any] | None:
        try:
            path = self._manifest_path(task_code, batch)
        except ExportBadRequest:
            return None
        if not path.is_file():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _save_manifest(self, task_code: str, batch: str, manifest: dict[str, Any]) -> None:
        path = self._manifest_path(task_code, batch)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = path.parent / f".{MANIFEST_NAME}.{uuid.uuid4().hex}.tmp"
        temp_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        os.replace(temp_path, path)

    def _manifest_summary(self, manifest: dict[str, Any]) -> dict[str, Any]:
        pipeline = manifest.get("pipeline") or {}
        config = manifest.get("output_config") or {}
        return {
            "task_code": manifest.get("task_code"),
            "batch": manifest.get("batch"),
            "status": manifest.get("status"),
            "record_count": int(manifest.get("record_count") or 0),
            "planned_record_count": int(manifest.get("planned_record_count") or 0),
            "invalid_count": int(manifest.get("invalid_count") or 0),
            "deduplicated_count": int(manifest.get("deduplicated_count") or 0),
            "input_count": int(manifest.get("input_count") or 0),
            "shard_count": int(manifest.get("shard_count") or 0),
            "pipeline_code": pipeline.get("code"),
            "config_code": config.get("code"),
            "config_version": config.get("version"),
            "updated_at": manifest.get("updated_at"),
            "failure_reason": manifest.get("failure_reason"),
        }

    def _reconcile(self, manifest: dict[str, Any]) -> None:
        """用磁盘上的分片校正清单，避免刷新后清单条数与实际文件不一致。"""
        batch_dir = self._batch_dir(str(manifest.get("task_code") or ""), str(manifest.get("batch") or ""))
        valid_total = 0
        problems: list[str] = []
        for shard in manifest.get("shards", []):
            path = batch_dir / str(shard.get("file") or "")
            if not path.is_file():
                shard["status"] = "缺失"
                problems.append(f"{shard.get('file')} 缺失")
                continue
            if int(shard.get("size_bytes") or -1) != path.stat().st_size:
                shard["status"] = "大小不一致"
                problems.append(f"{shard.get('file')} 大小不一致")
                continue
            if str(shard.get("status")) != "已完成":
                shard["status"] = "已完成"
            valid_total += int(shard.get("record_count") or 0)
        if problems:
            manifest["record_count"] = valid_total
            manifest["shard_count"] = sum(1 for shard in manifest.get("shards", []) if str(shard.get("status")) == "已完成")
            expected = int((manifest.get("plan") or {}).get("record_count") or 0)
            manifest["status"] = STATUS_PARTIAL if valid_total else STATUS_FAILED
            manifest["failure_reason"] = "；".join(problems) + "，请重新执行或恢复该批次"
            manifest["failed_shard_index"] = manifest.get("failed_shard_index") or (
                max((int(shard.get("index", 0)) for shard in manifest.get("shards", [])), default=0) + 1
            )
            manifest["planned_record_count"] = expected
            manifest["updated_at"] = _now()
            self._save_manifest(str(manifest.get("task_code") or ""), str(manifest.get("batch") or ""), manifest)
            self._sync_log(str(manifest.get("task_code") or ""), str(manifest.get("batch") or ""), manifest, None)

    def _sync_log(
        self,
        task_code: str,
        batch: str,
        manifest: dict[str, Any],
        attempt: dict[str, Any] | None,
    ) -> None:
        """采集日志与结果清单使用同一组计数，保证运行记录和清单能相互印证。"""
        with store.lock:
            rows = store.rows("storagelog")
            entry = next(
                (
                    row
                    for row in rows
                    if str(row.get("关联任务") or "") == task_code
                    and str(row.get("关联批次") or "") == batch
                ),
                None,
            )
            started = str((manifest.get("attempts") or [{}])[-1].get("started_at") or manifest.get("started_at") or "")
            finished = str(manifest.get("finished_at") or _now())
            try:
                elapsed_ms = max(
                    int((datetime.fromisoformat(finished) - datetime.fromisoformat(started)).total_seconds() * 1000),
                    0,
                )
            except ValueError:
                elapsed_ms = 0
            expected = int((manifest.get("plan") or {}).get("record_count") or 0)
            written = int(manifest.get("record_count") or 0)
            fields = {
                "关联任务": task_code,
                "关联批次": batch,
                "开始时间": started,
                "结束时间": finished,
                "请求总数": int(manifest.get("input_count") or 0),
                "成功条数": written,
                "失败条数": int(manifest.get("invalid_count") or 0) + max(expected - written, 0),
                "平均响应毫秒": elapsed_ms,
                "输出状态": manifest.get("status"),
                "输出清单": f"{task_code}/{batch}/{MANIFEST_NAME}",
                "未写入条数": max(expected - written, 0),
            }
            if entry is None:
                entry = {
                    "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                    "日志编号": f"STOR-{max((int(row.get('id', 0)) for row in rows), default=0) + 1:04d}",
                    "status": "已记录",
                    "pending": False,
                    "abnormal": manifest.get("status") != STATUS_SUCCESS,
                }
                rows.append(entry)
            entry.update(fields)
            entry["status"] = "已记录"
            entry["pending"] = False
            entry["abnormal"] = manifest.get("status") != STATUS_SUCCESS

    @staticmethod
    def _parse_fail_at(value: Any) -> int | None:
        if value in (None, "", 0):
            return None
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise ExportBadRequest("simulate_failure_at_shard 必须是大于 0 的整数")
        if parsed <= 0:
            raise ExportBadRequest("simulate_failure_at_shard 必须是大于 0 的整数")
        return parsed


class _ShardSerializer:
    """把一个输出记录序列化成字节，并按格式计算分片实际大小。"""

    def __init__(self, settings: ExportSettings, field_order: list[str]) -> None:
        self.settings = settings
        self.field_order = field_order
        self.format = settings.output_format
        self._header: bytes | None = None

    def header_bytes(self) -> bytes:
        if self._header is None:
            if self.format in {"csv", "tsv"} and self.field_order:
                delimiter = "\t" if self.format == "tsv" else self.settings.delimiter
                buffer = io.StringIO()
                writer = csv.writer(buffer, delimiter=delimiter, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
                writer.writerow(self.field_order)
                self._header = buffer.getvalue().encode(self.settings.encoding)
            else:
                self._header = b""
        return self._header

    def item_bytes(self, data: dict[str, Any]) -> bytes:
        if self.format == "ndjson":
            return (_json_dump(data) + "\n").encode(self.settings.encoding)
        if self.format == "json":
            return _json_dump(data).encode(self.settings.encoding)
        delimiter = "\t" if self.format == "tsv" else self.settings.delimiter
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=delimiter, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        row = []
        for field_name in self.field_order:
            value = data.get(field_name)
            if value is None:
                row.append("")
            elif isinstance(value, (dict, list)):
                row.append(_json_dump(value))
            else:
                row.append(str(value))
        writer.writerow(row)
        return buffer.getvalue().encode(self.settings.encoding)

    def size_with(self, count: int, item_bytes_total: int) -> int:
        """分片最终未压缩大小：容器头尾 + 记录字节 + 记录间分隔符。"""
        if self.format == "json":
            return 3 + item_bytes_total + max(count - 1, 0)
        if self.format in {"csv", "tsv"}:
            return len(self.header_bytes()) + item_bytes_total
        return item_bytes_total


def _format_extension(settings: ExportSettings) -> str:
    base = {"ndjson": ".ndjson", "json": ".json", "csv": ".csv", "tsv": ".tsv"}.get(settings.output_format, ".dat")
    return base + (".gz" if settings.compression == "gzip" else "")


def pipeline_field_order(pipeline_payload: dict[str, Any]) -> list[str]:
    """输出列顺序：字段映射里输出字段的声明顺序。"""
    mapping = pipeline_payload.get("field_mapping") or {}
    if isinstance(mapping, dict):
        return [str(value) for value in mapping.values()]
    return []


def _peek(items: Iterator[PlanItem]) -> tuple[Iterator[PlanItem], PlanItem | None]:
    """取迭代器首项用于推断列顺序，再把首项放回序列。"""
    iterator = iter(items)
    try:
        first = next(iterator)
    except StopIteration:
        return iter(()), None

    def chained() -> Iterator[PlanItem]:
        yield first
        for item in iterator:
            yield item

    return chained(), first
