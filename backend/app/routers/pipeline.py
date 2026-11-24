"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from fastapi import APIRouter, Query

from app.database import db
from app.pipeline import (
    check_alert, compute_aggregation, compute_cohort_comparison,
    compute_forecast, compute_funnel, compute_metric_snapshot,
    compute_retention, compute_segment, generate_report,
    ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(
    task_id: int,
    mode: str | None = Query(default=None, pattern="^(reuse|refresh)$"),
) -> dict:
    """对指定预测任务执行计算；带 mode 时续做被中断的那一轮。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id, mode)


@router.post("/forecast/{task_id}/prepare")
def api_prepare_forecast(task_id: int) -> dict:
    """完成预测的首次取数后中断，样本凭据落库但不产出结果。"""
    svc = service_for("forecast_model")
    return svc.prepare_forecast(task_id)


@router.post("/forecast/{task_id}/resume")
def api_resume_forecast(
    task_id: int,
    mode: str = Query(default="reuse", pattern="^(reuse|refresh)$"),
    run_id: int | None = Query(default=None, ge=1),
) -> dict:
    """续做被中断的预测：reuse 沿用首次样本，refresh 重新取满一组。"""
    svc = service_for("forecast_model")
    return svc.resume_forecast(task_id, mode, run_id)


@router.get("/forecast/{task_id}/runs")
def api_forecast_runs(task_id: int) -> dict:
    """列出预测任务每轮的取样凭据。"""
    svc = service_for("forecast_model")
    return {"ok": True, "items": svc.forecast_runs(task_id)}


@router.get("/forecast/{task_id}/runs/{run_id}")
def api_forecast_run_detail(task_id: int, run_id: int) -> dict:
    """复查某一轮预测实际使用的样本编号与计算顺序。"""
    svc = service_for("forecast_model")
    record = svc.review_run(task_id, run_id)
    if record is None:
        return {"ok": False, "message": f"预测任务 {task_id} 下没有取样凭据 {run_id}"}
    return {"ok": True, "run": record}


@router.get("/forecast/{task_id}/results")
def api_forecast_results(task_id: int) -> dict:
    """复查预测结果，并附上最新一轮的取样凭据。"""
    svc = service_for("forecast_model")
    return {"ok": True, **svc.review_results(task_id)}


@router.get("/forecast_model/{task_id}/forecasts")
def api_forecast_model_results(task_id: int) -> dict:
    """预测模型的结果入口，附带最新一轮取样凭据。"""
    return api_forecast_results(task_id)


@router.get("/forecast_model/{task_id}/run")
def api_forecast_model_latest_run(task_id: int) -> dict:
    """预测模型最新一轮的取样凭据。"""
    svc = service_for("forecast_model")
    return {"ok": True, "run": svc.latest_run(task_id)}


@router.get("/metrics/{metric_id}/trend")
def api_metric_trend(metric_id: int, limit: int | None = Query(default=None, ge=1)) -> dict:
    """获取指标趋势；不传 limit 时沿用既有默认行数上限。"""
    svc = service_for("metric_store")
    if limit is None:
        return svc.metric_trend(metric_id)
    return svc.metric_trend(metric_id, limit)


# ============================================================================
# 模块形态的补充入口：沿用既有模块 key 即可打开趋势或复查预测样本
# ============================================================================

@router.get("/metric_store/{metric_id}/trend")
def api_metric_store_trend(metric_id: int, limit: int | None = Query(default=None, ge=1)) -> dict:
    """指标存储的趋势入口。"""
    return api_metric_trend(metric_id, limit)


@router.post("/metric_store/{metric_id}/snapshot")
def api_metric_store_snapshot(metric_id: int) -> dict:
    """指标存储的快照计算入口，语义同 /metrics/{metric_id}/snapshot。"""
    return api_compute_snapshot(metric_id)


@router.get("/forecast_model/{task_id}/runs")
def api_forecast_model_runs(task_id: int) -> dict:
    """预测模型的取样凭据列表。"""
    return api_forecast_runs(task_id)


@router.get("/forecast_model/{task_id}/runs/{run_id}")
def api_forecast_model_run_detail(task_id: int, run_id: int) -> dict:
    """按轮次复查预测样本编号与计算顺序。"""
    return api_forecast_run_detail(task_id, run_id)


@router.post("/forecast_model/{task_id}/compute")
def api_forecast_model_compute(
    task_id: int,
    mode: str | None = Query(default=None, pattern="^(reuse|refresh)$"),
) -> dict:
    """预测模型的执行入口，语义同 /forecast/{task_id}/compute。"""
    return api_compute_forecast(task_id, mode)


@router.post("/forecast_model/{task_id}/prepare")
def api_forecast_model_prepare(task_id: int) -> dict:
    """预测模型的首次取数（取足样本后中断）入口。"""
    return api_prepare_forecast(task_id)


@router.post("/forecast_model/{task_id}/resume")
def api_forecast_model_resume(
    task_id: int,
    mode: str = Query(default="reuse", pattern="^(reuse|refresh)$"),
    run_id: int | None = Query(default=None, ge=1),
) -> dict:
    """预测模型的断点续做入口。"""
    return api_resume_forecast(task_id, mode, run_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/report/{report_id}/generate")
def api_generate_report(report_id: int) -> dict:
    """对指定报告模板执行生成。"""
    svc = service_for("data_report")
    return svc.generate(report_id)


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}