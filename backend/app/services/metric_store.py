"""指标存储领域服务：管理指标定义、执行快照计算并与基准对比。

真实数据流：接收指标定义 → 调用 pipeline.compute_metric_snapshot 计算当前值
→ 写入 metric_values 表 → 对比上一期基准值和历史序列 → 检测 z-score 异常
→ 下游告警策略消费者。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import compute_metric_snapshot
from app.sampling import TREND_LIMIT, fetch_metric_snapshot_window
from app.services.base import BaseModuleService


class MetricStoreService(BaseModuleService):
    MODULE = "metric_store"
    ENTITY = "指标快照"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["指标名称", "快照值", "快照时间", "口径版本", "对比基准"]
    REQUIRED_FIELDS = ["指标名称", "快照值", "快照时间"]
    STATUS_ORDER = ["已保存", "对比成功", "有异常"]
    ACTION_RULES = {"保存快照": "已保存", "标记异常": "有异常"}
    NEGATIVE_ACTIONS = []

    def list_metrics(self, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        """列出所有指标定义。"""
        total = db.count("metrics")
        rows = db.fetchall(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT ? OFFSET ?",
            [size, (page - 1) * size],
        )
        return rows, total

    def compute_snapshot(self, metric_id: int) -> dict[str, Any]:
        """对指定指标执行一次快照计算并存储。"""
        metric = db.fetchone("SELECT * FROM metrics WHERE id=?", [metric_id])
        if metric is None:
            return {"ok": False, "message": f"指标 {metric_id} 不存在"}
        result = compute_metric_snapshot(dict(metric))
        if result is None:
            return {"ok": False, "message": f"指标 {metric['metric_name']} 计算失败"}
        return {"ok": True, "result": result}

    def compute_all(self) -> dict[str, Any]:
        """对所有指标执行快照计算。"""
        metrics = db.fetchall("SELECT * FROM metrics")
        results = []
        for m in metrics:
            r = self.compute_snapshot(m["id"])
            results.append({
                "metric_name": m["metric_name"],
                "ok": r["ok"],
                "value": r["result"]["snapshot_value"] if r["ok"] else None,
                "status": r["result"]["comparison_status"] if r["ok"] else "failed",
            })
        return {"processed": len(metrics), "results": results}

    def metric_trend(self, metric_id: int, limit: int = TREND_LIMIT) -> dict[str, Any]:
        """获取指标的历史趋势数据。

        窗口读取走 ``app.sampling``：行数上限不变，展示方向仍是最近在前，
        同一采样时间的快照按编号倒序确定取舍。
        """
        metric = db.fetchone("SELECT * FROM metrics WHERE id=?", [metric_id])
        if metric is None:
            return {"trend": "no_data", "points": []}

        rows = fetch_metric_snapshot_window(metric_id, limit)
        values = [r["snapshot_value"] for r in rows]
        if not values:
            return {"trend": "no_data", "points": [], "metric_name": metric["metric_name"]}

        import statistics
        mean_v = statistics.mean(values)

        trend = "stable"
        if len(values) >= 2:
            if values[0] > mean_v * 1.1:
                trend = "upward"
            elif values[0] < mean_v * 0.9:
                trend = "downward"

        return {
            "metric_name": metric["metric_name"],
            "metric_type": metric["metric_type"],
            "unit": metric["unit"],
            "trend": trend,
            "latest": values[0],
            "mean": round(mean_v, 4),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "points": [{"time": r["snapshot_time"], "value": r["snapshot_value"],
                       "status": r["comparison_status"]} for r in rows],
        }

    def compare_calibrations(self, metric_id: int, v1: int, v2: int) -> dict[str, Any]:
        """对比同一指标在两个口径版本下的差异。"""
        snapshots_v1 = db.fetchall(
            "SELECT * FROM metric_values WHERE metric_id=? AND calibration_version=? ORDER BY snapshot_time DESC LIMIT 10",
            [metric_id, v1],
        )
        snapshots_v2 = db.fetchall(
            "SELECT * FROM metric_values WHERE metric_id=? AND calibration_version=? ORDER BY snapshot_time DESC LIMIT 10",
            [metric_id, v2],
        )

        import statistics
        mean_v1 = statistics.mean([s["snapshot_value"] for s in snapshots_v1]) if snapshots_v1 else 0
        mean_v2 = statistics.mean([s["snapshot_value"] for s in snapshots_v2]) if snapshots_v2 else 0
        diff_pct = round((mean_v1 - mean_v2) / max(abs(mean_v2), 0.0001) * 100, 2)

        return {
            "metric_id": metric_id,
            "version_a": v1,
            "version_b": v2,
            "mean_a": round(mean_v1, 4),
            "mean_b": round(mean_v2, 4),
            "diff_pct": diff_pct,
            "significant": abs(diff_pct) > 5,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_metrics"] = db.count("metrics")
        snapshot["total_snapshots"] = db.count("metric_values")
        abnormal = db.count("metric_values", "comparison_status='有异常'")
        snapshot["abnormal_snapshots"] = abnormal
        return snapshot