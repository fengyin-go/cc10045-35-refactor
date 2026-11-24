"""数据仓库：兼容层，委托给 SQLite 数据库。

保留旧接口以兼容 base.py 和旧路由，新代码应直接使用 app.database.db。
"""
from __future__ import annotations

from typing import Any

from app.database import db as _db


class Store:
    """SQLite-backed 兼容层，提供与旧内存 Store 相同的接口。"""

    def __init__(self) -> None:
        self._db = _db

    def module_names(self) -> list[str]:
        tables = self._db.table_names()
        business_tables = [t for t in tables if not t.startswith("_") and t not in
                          ("aggregation_results", "metric_values", "segment_members",
                           "funnel_results", "retention_results", "cube_cells",
                           "forecast_results", "forecast_sample_runs",
                           "report_outputs", "alert_log")]
        return sorted(business_tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        table_map = {
            "realtime_agg": "aggregation_windows",
            "event_tracker": "events",
            "dashboard_builder": "dashboards",
            "query_cache": "aggregation_windows",
            "segment_engine": "segments",
            "metric_store": "metrics",
            "drill_path": "drill_rules",
            "alert_policy": "alerts",
            "data_report": "reports",
            "anomaly_analysis": "anomaly_analyses",
            "forecast_model": "forecast_tasks",
            "cohort_compare": "cohort_comparisons",
            "data_export": "exports",
            "data_subscription": "subscriptions",
            "query_history": "query_log",
            "report_snapshot": "snapshots",
            "data_api": "api_endpoints",
            "retention_analysis": "retention_tasks",
            "funnel_analyzer": "funnels",
            "cube_builder": "cubes",
        }
        table = table_map.get(module, module)
        try:
            return self._db.fetchall(f"SELECT * FROM {table} ORDER BY id")
        except Exception:
            return []

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        table_map = {
            "realtime_agg": "aggregation_windows",
            "event_tracker": "events",
            "dashboard_builder": "dashboards",
            "query_cache": "aggregation_windows",
            "segment_engine": "segments",
            "metric_store": "metrics",
            "drill_path": "drill_rules",
            "alert_policy": "alerts",
            "data_report": "reports",
            "anomaly_analysis": "anomaly_analyses",
            "forecast_model": "forecast_tasks",
            "cohort_compare": "cohort_comparisons",
            "data_export": "exports",
            "data_subscription": "subscriptions",
            "query_history": "query_log",
            "report_snapshot": "snapshots",
            "data_api": "api_endpoints",
            "retention_analysis": "retention_tasks",
            "funnel_analyzer": "funnels",
            "cube_builder": "cubes",
        }
        table = table_map.get(module, module)
        try:
            return self._db.fetchone(f"SELECT * FROM {table} WHERE id=?", [entry_id])
        except Exception:
            return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("status") not in ("已停用", "已下线", "已归档", "已冻结", "已过期", "已关闭", "已暂停")),
                "abnormal": 0,
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "数据表", "value": sum(int(item["created"]) for item in modules)},
            {"label": "活跃记录", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "事件总量", "value": self._db.count("events")},
        ]
        return {"cards": cards, "modules": modules}


store = Store()