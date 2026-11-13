"""分片输出接口：执行输出任务、查看结果清单、下载分片与恢复失败批次。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import FileResponse

from app.schemas import ExportRunRequest, PageResult
from app.services.exportrun import ExportBadRequest, ExportNotFound, ExportRunService

router = APIRouter(prefix="/api/exportrun", tags=["分片输出"])

service = ExportRunService()


def _payload(manifest: dict[str, Any]) -> dict[str, Any]:
    """结果清单对外结构：分片文件、记录数、字段映射、输出配置版本。"""
    return {
        "task_code": manifest.get("task_code"),
        "batch": manifest.get("batch"),
        "status": manifest.get("status"),
        "failure_reason": manifest.get("failure_reason"),
        "failed_shard_index": manifest.get("failed_shard_index"),
        "record_count": int(manifest.get("record_count") or 0),
        "planned_record_count": int(manifest.get("planned_record_count") or 0),
        "shard_count": int(manifest.get("shard_count") or 0),
        "input_count": int(manifest.get("input_count") or 0),
        "invalid_count": int(manifest.get("invalid_count") or 0),
        "deduplicated_count": int(manifest.get("deduplicated_count") or 0),
        "pipeline": manifest.get("pipeline"),
        "output_config": manifest.get("output_config"),
        "shards": manifest.get("shards", []),
        "attempts": manifest.get("attempts", []),
        "started_at": manifest.get("started_at"),
        "finished_at": manifest.get("finished_at"),
        "updated_at": manifest.get("updated_at"),
    }


@router.get("", response_model=PageResult[dict])
def list_runs(
    task_code: str | None = Query(default=None, description="按任务编号过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """列出已有结果清单；刷新或重新进入页面时用这里的数据恢复展示。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_manifests(task_code=task_code, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/{task_code}/{batch}", response_model=dict)
def get_manifest(task_code: str, batch: str) -> dict:
    """读取结果清单；每次读取都会用磁盘分片校正记录数，保证与已写入分片一致。"""
    manifest = service.get_manifest(task_code, batch)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"任务 {task_code} 批次 {batch} 的结果清单不存在")
    return _payload(manifest)


@router.get("/{task_code}/{batch}/shards/{index}")
def download_shard(task_code: str, batch: str, index: int) -> FileResponse:
    """下载单个分片文件；清单里没有登记或文件缺失时返回明确原因。"""
    path = service.shard_file(task_code, batch, index)
    if path is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch} 不存在分片 {index}")
    return FileResponse(path, filename=path.name)


@router.post("")
def run_export(payload: ExportRunRequest) -> dict:
    """执行分片输出；同一任务 + 批次重复执行会覆盖同一份清单，不产生重复分片。"""
    try:
        manifest = service.execute(payload.model_dump(exclude_none=True))
    except ExportBadRequest as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ExportNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _payload(manifest)


@router.post("/{task_code}/{batch}/resume")
def resume_export(task_code: str, batch: str, payload: ExportRunRequest | None = None) -> dict:
    """从失败位置恢复批次：复用已落盘的计划，只继续写入未完成的分片。"""
    values = payload.model_dump(exclude_none=True) if payload else {}
    values.update({"task_code": task_code, "batch": batch, "resume": True})
    values.pop("pipeline_code", None)
    values.pop("export_code", None)
    try:
        manifest = service.execute(values)
    except ExportBadRequest as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except ExportNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return _payload(manifest)
