"""SQLite 存储层：连接管理、表定义、查询和执行，替代纯内存 Store。

真实分析平台使用 WAL 模式 SQLite，提供事件、聚合、指标、分群、漏斗等全量数据表。
所有写操作显式提交，读操作使用 WAL 读视图保证并发安全。
"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Sequence

DB_DIR = Path(os.environ.get("CC10045_DB_DIR", Path(__file__).resolve().parent.parent.parent / "data"))
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "cc10045.db"

DDL_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS _schema (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",

    # ==================== Event & Ingestion Tier ====================
    """CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_name TEXT NOT NULL,
        event_attrs TEXT NOT NULL DEFAULT '{}',
        trigger_condition TEXT,
        source_module TEXT NOT NULL DEFAULT '',
        occurred_at TEXT NOT NULL,
        ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
        batch_id TEXT,
        payload_size INTEGER DEFAULT 0
    )""",
    "CREATE INDEX IF NOT EXISTS idx_events_name_time ON events(event_name, occurred_at)",
    "CREATE INDEX IF NOT EXISTS idx_events_batch ON events(batch_id)",

    # ==================== Aggregation Tier ====================
    """CREATE TABLE IF NOT EXISTS aggregation_windows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_name TEXT NOT NULL,
        data_source_module TEXT NOT NULL,
        dimensions TEXT NOT NULL DEFAULT '[]',
        agg_function TEXT NOT NULL,
        window_type TEXT NOT NULL CHECK(window_type IN ('tumbling','sliding','session')),
        window_size_seconds INTEGER NOT NULL DEFAULT 60,
        status TEXT NOT NULL DEFAULT '草稿',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS aggregation_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        window_id INTEGER NOT NULL,
        window_start TEXT NOT NULL,
        window_end TEXT NOT NULL,
        dimension_key TEXT NOT NULL DEFAULT '',
        computed_value REAL NOT NULL DEFAULT 0.0,
        event_count INTEGER NOT NULL DEFAULT 0,
        computation_version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (window_id) REFERENCES aggregation_windows(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_agg_results_window ON aggregation_results(window_id, window_start)",
    "CREATE INDEX IF NOT EXISTS idx_agg_results_dim ON aggregation_results(dimension_key)",

    # ==================== Metric Tier ====================
    """CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_name TEXT NOT NULL UNIQUE,
        metric_type TEXT NOT NULL CHECK(metric_type IN ('counter','gauge','ratio','composite')),
        source_module TEXT NOT NULL,
        computation_sql TEXT,
        calibration_version INTEGER NOT NULL DEFAULT 1,
        unit TEXT DEFAULT '',
        description TEXT DEFAULT '',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS metric_values (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metric_id INTEGER NOT NULL,
        snapshot_value REAL NOT NULL,
        snapshot_time TEXT NOT NULL,
        calibration_version INTEGER NOT NULL DEFAULT 1,
        baseline_value REAL,
        comparison_status TEXT DEFAULT '已保存',
        z_score REAL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (metric_id) REFERENCES metrics(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_mv_metric_time ON metric_values(metric_id, snapshot_time)",

    # ==================== Segment Tier ====================
    """CREATE TABLE IF NOT EXISTS segments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        segment_name TEXT NOT NULL,
        conditions TEXT NOT NULL DEFAULT '{}',
        population_attr TEXT,
        estimated_size INTEGER DEFAULT 0,
        status TEXT NOT NULL DEFAULT '草稿',
        last_computed_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS segment_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        segment_id INTEGER NOT NULL,
        member_key TEXT NOT NULL,
        member_attrs TEXT DEFAULT '{}',
        assigned_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (segment_id) REFERENCES segments(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_sm_segment ON segment_members(segment_id)",
    "CREATE INDEX IF NOT EXISTS idx_sm_member ON segment_members(member_key)",

    # ==================== Funnel Tier ====================
    """CREATE TABLE IF NOT EXISTS funnels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        funnel_name TEXT NOT NULL,
        steps TEXT NOT NULL DEFAULT '[]',
        conversion_window_hours INTEGER NOT NULL DEFAULT 24,
        segment_condition TEXT,
        status TEXT NOT NULL DEFAULT '草稿',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS funnel_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        funnel_id INTEGER NOT NULL,
        step_index INTEGER NOT NULL,
        step_name TEXT NOT NULL,
        entry_count INTEGER NOT NULL,
        exit_count INTEGER NOT NULL,
        conversion_rate REAL NOT NULL DEFAULT 0.0,
        drop_reasons TEXT DEFAULT '{}',
        computed_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (funnel_id) REFERENCES funnels(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fr_funnel ON funnel_results(funnel_id)",

    # ==================== Retention Tier ====================
    """CREATE TABLE IF NOT EXISTS retention_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT NOT NULL,
        behavior_event TEXT NOT NULL,
        retention_days INTEGER NOT NULL DEFAULT 7,
        segment_dimension TEXT,
        status TEXT NOT NULL DEFAULT '待计算',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS retention_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        day_index INTEGER NOT NULL,
        cohort_date TEXT NOT NULL,
        cohort_size INTEGER NOT NULL,
        retained_count INTEGER NOT NULL,
        retention_rate REAL NOT NULL,
        computed_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (task_id) REFERENCES retention_tasks(id)
    )""",

    # ==================== Cube / OLAP Tier ====================
    """CREATE TABLE IF NOT EXISTS cubes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cube_name TEXT NOT NULL,
        fact_table TEXT NOT NULL,
        dimensions TEXT NOT NULL DEFAULT '[]',
        measure_expr TEXT NOT NULL,
        precomputation_status TEXT NOT NULL DEFAULT '草稿',
        row_count INTEGER DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS cube_cells (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cube_id INTEGER NOT NULL,
        dimension_values TEXT NOT NULL DEFAULT '{}',
        measure_value REAL NOT NULL DEFAULT 0.0,
        computed_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (cube_id) REFERENCES cubes(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cc_cube ON cube_cells(cube_id)",

    # ==================== Anomaly Tier ====================
    """CREATE TABLE IF NOT EXISTS anomaly_analyses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_name TEXT NOT NULL,
        anomaly_time TEXT NOT NULL,
        attribution_dimensions TEXT NOT NULL DEFAULT '[]',
        contribution_ranking TEXT DEFAULT '{}',
        conclusion TEXT,
        status TEXT NOT NULL DEFAULT '待分析',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    # ==================== Forecast Tier ====================
    """CREATE TABLE IF NOT EXISTS forecast_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_name TEXT NOT NULL,
        target_metric TEXT NOT NULL,
        forecast_period_days INTEGER NOT NULL DEFAULT 7,
        forecast_method TEXT NOT NULL,
        baseline_days INTEGER NOT NULL DEFAULT 30,
        status TEXT NOT NULL DEFAULT '待执行',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS forecast_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        forecast_date TEXT NOT NULL,
        predicted_value REAL NOT NULL,
        lower_bound REAL,
        upper_bound REAL,
        confidence REAL DEFAULT 0.95,
        computed_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (task_id) REFERENCES forecast_tasks(id)
    )""",

    """CREATE TABLE IF NOT EXISTS forecast_sample_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL,
        run_no INTEGER NOT NULL DEFAULT 1,
        metric_id INTEGER,
        status TEXT NOT NULL DEFAULT '执行中',
        method TEXT DEFAULT '',
        source TEXT NOT NULL DEFAULT '窗口取样',
        sample_ids TEXT NOT NULL DEFAULT '[]',
        sample_values TEXT NOT NULL DEFAULT '[]',
        sample_times TEXT NOT NULL DEFAULT '[]',
        calculation_order TEXT NOT NULL DEFAULT '[]',
        results_json TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        completed_at TEXT,
        FOREIGN KEY (task_id) REFERENCES forecast_tasks(id)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_fsr_task ON forecast_sample_runs(task_id, id)",
    "CREATE INDEX IF NOT EXISTS idx_fsr_status ON forecast_sample_runs(task_id, status)",

    # ==================== Comparison & Report Tier ====================
    """CREATE TABLE IF NOT EXISTS cohort_comparisons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        comparison_name TEXT NOT NULL,
        segment_a_id INTEGER,
        segment_b_id INTEGER,
        compare_dimensions TEXT NOT NULL DEFAULT '[]',
        significance_level REAL DEFAULT 0.05,
        status TEXT NOT NULL DEFAULT '待计算',
        result_summary TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_name TEXT NOT NULL,
        metric_ids TEXT NOT NULL DEFAULT '[]',
        report_period TEXT NOT NULL,
        output_format TEXT NOT NULL DEFAULT 'json',
        auto_send INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT '草稿',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS report_outputs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL,
        output_content TEXT NOT NULL DEFAULT '{}',
        generated_at TEXT NOT NULL DEFAULT (datetime('now')),
        FOREIGN KEY (report_id) REFERENCES reports(id)
    )""",

    # ==================== Operational Tier ====================
    """CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_name TEXT NOT NULL,
        monitor_metric TEXT NOT NULL,
        alert_condition TEXT NOT NULL,
        notify_channel TEXT DEFAULT '',
        consecutive_threshold INTEGER NOT NULL DEFAULT 3,
        status TEXT NOT NULL DEFAULT '待生效',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS alert_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_id INTEGER NOT NULL,
        triggered_at TEXT NOT NULL DEFAULT (datetime('now')),
        metric_value REAL,
        threshold_value REAL,
        resolved INTEGER DEFAULT 0,
        FOREIGN KEY (alert_id) REFERENCES alerts(id)
    )""",

    """CREATE TABLE IF NOT EXISTS query_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        executor TEXT NOT NULL DEFAULT 'system',
        dataset TEXT NOT NULL,
        query_summary TEXT,
        row_count INTEGER DEFAULT 0,
        execution_time_ms INTEGER DEFAULT 0,
        status TEXT NOT NULL DEFAULT '正常',
        executed_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sub_name TEXT NOT NULL,
        metric_id INTEGER,
        push_condition TEXT NOT NULL DEFAULT '{}',
        push_channel TEXT DEFAULT '',
        status TEXT NOT NULL DEFAULT '待生效',
        last_pushed_at TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS exports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        export_name TEXT NOT NULL,
        query_condition TEXT DEFAULT '{}',
        export_format TEXT NOT NULL DEFAULT 'csv',
        status TEXT NOT NULL DEFAULT '排队中',
        file_size_bytes INTEGER DEFAULT 0,
        output_path TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        snapshot_name TEXT NOT NULL,
        report_id INTEGER,
        snapshot_content TEXT NOT NULL DEFAULT '{}',
        comparison_diff TEXT,
        status TEXT NOT NULL DEFAULT '已保存',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS dashboards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dashboard_name TEXT NOT NULL,
        chart_configs TEXT NOT NULL DEFAULT '[]',
        refresh_interval_seconds INTEGER DEFAULT 30,
        data_scope TEXT DEFAULT '',
        share_status TEXT DEFAULT 'private',
        status TEXT NOT NULL DEFAULT '草稿',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS drill_rules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rule_name TEXT NOT NULL,
        start_metric TEXT NOT NULL,
        drill_dimension TEXT NOT NULL,
        target_module TEXT NOT NULL,
        default_granularity TEXT DEFAULT 'day',
        status TEXT NOT NULL DEFAULT '草稿',
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",

    """CREATE TABLE IF NOT EXISTS api_endpoints (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        endpoint_name TEXT NOT NULL,
        related_metric TEXT,
        http_method TEXT NOT NULL DEFAULT 'GET',
        response_format TEXT NOT NULL DEFAULT 'json',
        status TEXT NOT NULL DEFAULT '开发中',
        daily_calls INTEGER DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )""",
]


class AnalyticsDB:
    """SQLite-backed analytics database with connection pooling and WAL mode."""

    _instance: AnalyticsDB | None = None
    _lock = threading.Lock()

    def __init__(self) -> None:
        self._local = threading.local()
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        for stmt in DDL_STATEMENTS:
            conn.execute(stmt)
        conn.commit()
        conn.close()

    @classmethod
    def get(cls) -> AnalyticsDB:
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    def _get_conn(self) -> sqlite3.Connection:
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
            self._local.conn.row_factory = sqlite3.Row
        return self._local.conn

    @contextmanager
    def tx(self) -> Generator[sqlite3.Connection, None, None]:
        conn = self._get_conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        conn = self._get_conn()
        cursor = conn.execute(sql, params)
        conn.commit()
        return cursor

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> sqlite3.Cursor:
        conn = self._get_conn()
        cursor = conn.executemany(sql, params_list)
        conn.commit()
        return cursor

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        conn = self._get_conn()
        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> dict[str, Any] | None:
        conn = self._get_conn()
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None

    def insert(self, table: str, data: dict[str, Any]) -> int:
        columns = ", ".join(data.keys())
        placeholders = ", ".join("?" for _ in data)
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        cursor = self.execute(sql, tuple(data.values()))
        return cursor.lastrowid or 0

    def update(self, table: str, data: dict[str, Any], where: str, params: Sequence[Any] = ()) -> int:
        sets = ", ".join(f"{k} = ?" for k in data)
        sql = f"UPDATE {table} SET {sets} WHERE {where}"
        cursor = self.execute(sql, tuple(data.values()) + tuple(params))
        return cursor.rowcount

    def delete(self, table: str, where: str, params: Sequence[Any] = ()) -> int:
        sql = f"DELETE FROM {table} WHERE {where}"
        cursor = self.execute(sql, params)
        return cursor.rowcount

    def count(self, table: str, where: str = "1=1", params: Sequence[Any] = ()) -> int:
        row = self.fetchone(f"SELECT COUNT(*) as cnt FROM {table} WHERE {where}", params)
        return int(row["cnt"]) if row else 0

    def table_names(self) -> list[str]:
        rows = self.fetchall("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        return [r["name"] for r in rows]

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


db = AnalyticsDB.get()