"""预测模型领域服务：基于历史指标数据执行趋势和时间序列预测。

真实数据流：定义预测任务（指标/周期/方法/基线天数）
→ pipeline.compute_forecast 执行 SES/移动平均/线性趋势预测
→ 写入 forecast_results → 下游报告和告警消费者。

取样统一走 ``app.sampling``，每次预测都会在 ``forecast_sample_runs`` 里留下一份
可复查的样本凭据（编号、取值、采样时间和计算顺序）：

* 首次取数后中断，续做可以沿用第一次选中的完整样本，也可以按当前数据重新
  取满一组；两次序列绝不拼接。
* 每轮预测各占一条凭据，新开一轮不会覆盖此前已完成轮次的样本与结果。
"""
from __future__ import annotations

import json
from typing import Any

from app import pipeline
from app.database import db
from app.pipeline import compute_forecast
from app.sampling import (
    FORECAST_MIN_SAMPLES,
    fetch_snapshot_sample_by_ids,
    forecast_window_size,
    sample_metric_snapshots,
    snapshot_credential,
)
from app.services.base import BaseModuleService

#: 初次导入的预测函数，用来识别运行期被替换（例如模拟中断）的情况。
_IMPORTED_COMPUTE_FORECAST = compute_forecast

#: 续做时沿用的“已取足样本、尚未产出结果”状态。
INTERRUPTED_STATUSES = ("执行中", "执行中断")
#: 续做的取样方式：沿用首次选中的样本 / 按当前窗口重新取满。
RESUME_MODE_REUSE = "reuse"
RESUME_MODE_REFRESH = "refresh"


class ForecastModelService(BaseModuleService):
    MODULE = "forecast_model"
    ENTITY = "预测任务"
    OPERATION_TERM = "预测"
    LIST_FIELDS = ["任务名称", "预测指标", "预测周期", "预测方式", "历史基线天数"]
    REQUIRED_FIELDS = ["任务名称", "预测指标", "预测周期"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "执行失败"]
    ACTION_RULES = {"启动预测": "已完成", "取消预测": "执行失败"}
    NEGATIVE_ACTIONS = []

    # ================================================================
    # 执行 / 中断 / 续做
    # ================================================================

    def run_forecast(self, task_id: int, mode: str | None = None) -> dict[str, Any]:
        """执行预测任务。

        默认入口语义不变：正常开启一轮新预测。若该任务最近一轮只取完数就被
        中断，则先接着那一轮续做（默认沿用首次样本）。``mode="reuse"`` /
        ``mode="refresh"`` 可明确指定续做方式；没有可续做记录时按新预测处理。
        """
        task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}

        want_resume = mode in (RESUME_MODE_REUSE, RESUME_MODE_REFRESH) or mode is None
        if want_resume and self._latest_interrupted_run(task_id) is not None:
            return self.resume_forecast(task_id, mode or RESUME_MODE_REUSE)

        # 既有行为：指标未找到时直接返回空结果，不动已有结果行。
        metric = self._resolve_metric(task)
        if metric is None:
            db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])
            return {
                "ok": True,
                "task_name": task["task_name"],
                "method": task["forecast_method"],
                "forecasts": [],
                "run_id": None,
                "sample_ids": [],
            }

        window = self._window_size(task)
        samples = sample_metric_snapshots(metric["id"], window)
        self._reset_results(task_id)
        if len(samples) < FORECAST_MIN_SAMPLES:
            # 保留既有行为：不足最小样本量时返回空结果。
            db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])
            return {
                "ok": True,
                "task_name": task["task_name"],
                "method": task["forecast_method"],
                "forecasts": [],
                "run_id": None,
                "sample_ids": [],
            }

        run = self._create_run(
            task, metric["id"], samples, source="窗口取样", reset_results=False)
        db.update("forecast_tasks", {"status": "执行中"}, "id=?", [task_id])
        try:
            results = _run_forecast_pipeline(task, run["sample_ids"])
        except Exception:
            self._mark_run(run["id"], status="执行中断", results=None)
            raise
        self._complete_run(run["id"], results, samples)
        db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])

        return {
            "ok": True,
            "task_name": task["task_name"],
            "method": task["forecast_method"],
            "forecasts": results,
            **_run_summary(run["id"]),
        }

    def prepare_forecast(self, task_id: int) -> dict[str, Any]:
        """只完成首次取数，不产出预测结果（用于复现“取足样本后中断”）。

        取样凭据会立即落库，任务保持 ``执行中``；之后可以调用
        :meth:`resume_forecast` 接着做，不会把中断前后的样本拼在一起。
        """
        task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}

        metric = self._resolve_metric(task)
        if metric is None:
            return {"ok": False, "message": f"预测指标 {task['target_metric']} 不存在"}

        samples = sample_metric_snapshots(metric["id"], self._window_size(task))
        if len(samples) < FORECAST_MIN_SAMPLES:
            self._reset_results(task_id)
            db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])
            return {
                "ok": True,
                "task_name": task["task_name"],
                "method": task["forecast_method"],
                "forecasts": [],
                "run_id": None,
                "sample_ids": [],
            }

        run = self._create_run(
            task, metric["id"], samples, source="窗口取样", reset_results=True)
        db.update("forecast_tasks", {"status": "执行中"}, "id=?", [task_id])
        return {
            "ok": True,
            "interrupted": True,
            "task_name": task["task_name"],
            "method": task["forecast_method"],
            "run_id": run["id"],
            "status": "执行中",
            **_run_summary(run["id"]),
        }

    def resume_forecast(
        self,
        task_id: int,
        mode: str = RESUME_MODE_REUSE,
        run_id: int | None = None,
    ) -> dict[str, Any]:
        """续做同一次被中断的预测。

        ``mode="reuse"`` 沿用第一次选中的样本编号；``mode="refresh"`` 按当前
        窗口重新取满一组。两种方式都只使用一组完整样本，不会拼接。
        ``run_id`` 可指定要续做的中断轮次，缺省时取最近一轮。
        """
        if mode not in (RESUME_MODE_REUSE, RESUME_MODE_REFRESH):
            return {"ok": False, "message": f"不支持的续做方式：{mode}"}

        task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
        if task is None:
            return {"ok": False, "message": f"预测任务 {task_id} 不存在"}

        run = self._interrupted_run(task_id, run_id)
        if run is None:
            target = f"取样凭据 {run_id}" if run_id is not None else "待续做的中断记录"
            return {"ok": False, "message": f"预测任务 {task_id} 没有{target}"}

        metric = self._resolve_metric(task)
        if metric is None:
            return {"ok": False, "message": f"预测指标 {task['target_metric']} 不存在"}

        if mode == RESUME_MODE_REUSE:
            source = "续用首次样本"
            samples = fetch_snapshot_sample_by_ids(
                metric["id"], _load_ids(run["sample_ids"]))
            if len(samples) < FORECAST_MIN_SAMPLES:
                # 已选样本被删除时保持“空结果”，绝不拿新样本补齐旧序列。
                self._reset_results(task_id)
                self._mark_run(run["id"], status="样本不足", results=None)
                db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])
                return {
                    "ok": True,
                    "task_name": task["task_name"],
                    "method": task["forecast_method"],
                    "forecasts": [],
                    "run_id": run["id"],
                    **_run_summary(run["id"]),
                }
        else:
            source = "重新取样"
            samples = sample_metric_snapshots(metric["id"], self._window_size(task))
            if len(samples) < FORECAST_MIN_SAMPLES:
                self._reset_results(task_id)
                self._mark_run(run["id"], status="样本不足", results=None)
                db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])
                return {
                    "ok": True,
                    "task_name": task["task_name"],
                    "method": task["forecast_method"],
                    "forecasts": [],
                    "run_id": run["id"],
                    **_run_summary(run["id"]),
                }

        db.update("forecast_tasks", {"status": "执行中"}, "id=?", [task_id])
        self._reset_results(task_id)
        credential = snapshot_credential(samples)
        db.update("forecast_sample_runs", {
            "sample_ids": json.dumps(credential["sample_ids"]),
            "sample_values": json.dumps(credential["sample_values"]),
            "sample_times": json.dumps(credential["sample_times"], ensure_ascii=False),
            "calculation_order": json.dumps(credential["calculation_order"], ensure_ascii=False),
            "method": task.get("预测方式", task.get("forecast_method", "")),
            "source": source,
        }, "id=?", [run["id"]])

        try:
            results = _run_forecast_pipeline(task, credential["sample_ids"])
        except Exception:
            self._mark_run(run["id"], status="执行中断", results=None)
            raise
        self._complete_run(run["id"], results, samples, source=source)
        db.update("forecast_tasks", {"status": "已完成"}, "id=?", [task_id])

        return {
            "ok": True,
            "task_name": task["task_name"],
            "method": task["forecast_method"],
            "forecasts": results,
            "resume_mode": mode,
            **_run_summary(run["id"]),
        }

    # ================================================================
    # 结果与凭据复查
    # ================================================================

    def forecast_data(self, task_id: int) -> list[dict[str, Any]]:
        """获取预测结果。"""
        return db.fetchall(
            "SELECT * FROM forecast_results WHERE task_id=? ORDER BY forecast_date ASC",
            [task_id],
        )

    def forecast_runs(self, task_id: int) -> list[dict[str, Any]]:
        """列出任务的全部取样凭据，最新一轮在前。"""
        rows = db.fetchall(
            "SELECT * FROM forecast_sample_runs WHERE task_id=? ORDER BY run_no DESC, id DESC",
            [task_id],
        )
        return [_run_record(row) for row in rows]

    def review_run(self, task_id: int, run_id: int) -> dict[str, Any] | None:
        """复查某一轮预测实际使用的样本编号与计算顺序。"""
        row = db.fetchone(
            "SELECT * FROM forecast_sample_runs WHERE id=? AND task_id=?",
            [run_id, task_id],
        )
        if row is None:
            return None
        return _run_record(row)

    def latest_run(self, task_id: int) -> dict[str, Any] | None:
        row = db.fetchone(
            "SELECT * FROM forecast_sample_runs WHERE task_id=? ORDER BY run_no DESC, id DESC LIMIT 1",
            [task_id],
        )
        return _run_record(row) if row else None

    def review_results(self, task_id: int) -> dict[str, Any]:
        """复查预测结果并带上最新一轮的样本凭据。

        每个结果行都标注 ``run_id``，方便和 ``calculation_order`` 对齐核对。
        """
        run = self.latest_run(task_id)
        items = self.forecast_data(task_id)
        if run is not None:
            items = [{**item, "run_id": run["run_id"]} for item in items]
        return {"items": items, "run": run}

    def evaluate_accuracy(self, task_id: int) -> dict[str, Any]:
        """评估预测准确率：对比预测值与实际值。"""
        task = db.fetchone("SELECT * FROM forecast_tasks WHERE id=?", [task_id])
        if task is None:
            return {"ok": False, "message": "任务不存在"}

        forecasts = db.fetchall(
            "SELECT * FROM forecast_results WHERE task_id=? ORDER BY forecast_date ASC LIMIT 7",
            [task_id],
        )

        metric = db.fetchone(
            "SELECT id FROM metrics WHERE metric_name LIKE ?",
            [f"%{task['target_metric']}%"],
        )
        if not metric:
            return {"ok": False, "message": "关联指标不存在"}

        actuals = db.fetchall(
            "SELECT snapshot_value, snapshot_time FROM metric_values WHERE metric_id=? "
            "ORDER BY snapshot_time DESC, id DESC LIMIT 7",
            [metric["id"]],
        )

        errors = []
        for f, a in zip(forecasts, reversed(actuals)):
            err = abs(f["predicted_value"] - a["snapshot_value"]) / max(abs(a["snapshot_value"]), 0.0001)
            errors.append(err)

        mape = sum(errors) / max(len(errors), 1) * 100

        return {
            "ok": True,
            "task_name": task["task_name"],
            "mape_pct": round(mape, 2),
            "accuracy_rating": "good" if mape < 10 else ("acceptable" if mape < 20 else "poor"),
            "comparisons": len(errors),
        }

    # ================================================================
    # 内部工具
    # ================================================================

    @staticmethod
    def _reset_results(task_id: int) -> None:
        """开新一轮时清掉任务上的旧结果行，历史轮次的凭据另表保存。"""
        db.delete("forecast_results", "task_id=?", [task_id])

    @staticmethod
    def _window_size(task: dict[str, Any]) -> int:
        """预测的行窗口：五，且不超过任务的历史基线天数。"""
        return forecast_window_size(task.get("历史基线天数", task.get("baseline_days", 30)))

    @staticmethod
    def _resolve_metric(task: dict[str, Any]) -> dict[str, Any] | None:
        target = task.get("预测指标", task.get("target_metric", ""))
        return db.fetchone("SELECT id FROM metrics WHERE metric_name LIKE ?", [f"%{target}%"])

    def _create_run(
        self,
        task: dict[str, Any],
        metric_id: int,
        samples: list[dict[str, Any]],
        *,
        source: str,
        reset_results: bool = True,
    ) -> dict[str, Any]:
        if reset_results:
            self._reset_results(task["id"])
        credential = snapshot_credential(samples)
        run_no = int(db.fetchone(
            "SELECT COALESCE(MAX(run_no), 0) AS n FROM forecast_sample_runs WHERE task_id=?",
            [task["id"]],
        )["n"]) + 1
        run_id = db.insert("forecast_sample_runs", {
            "task_id": task["id"],
            "run_no": run_no,
            "metric_id": metric_id,
            "status": "执行中",
            "method": task.get("预测方式", task.get("forecast_method", "")),
            "source": source,
            "sample_ids": json.dumps(credential["sample_ids"]),
            "sample_values": json.dumps(credential["sample_values"]),
            "sample_times": json.dumps(credential["sample_times"], ensure_ascii=False),
            "calculation_order": json.dumps(credential["calculation_order"], ensure_ascii=False),
            "results_json": "[]",
        })
        return {
            "id": run_id,
            "run_no": run_no,
            "sample_ids": credential["sample_ids"],
        }

    @staticmethod
    def _complete_run(
        run_id: int,
        results: list[dict[str, Any]],
        samples: list[dict[str, Any]],
        *,
        source: str | None = None,
    ) -> None:
        credential = snapshot_credential(samples)
        values: dict[str, Any] = {
            "status": "已完成",
            "completed_at": db.now(),
            "results_json": json.dumps(results, ensure_ascii=False),
            "sample_ids": json.dumps(credential["sample_ids"]),
            "sample_values": json.dumps(credential["sample_values"]),
            "sample_times": json.dumps(credential["sample_times"], ensure_ascii=False),
            "calculation_order": json.dumps(credential["calculation_order"], ensure_ascii=False),
        }
        if source is not None:
            values["source"] = source
        db.update("forecast_sample_runs", values, "id=?", [run_id])

    @staticmethod
    def _mark_run(run_id: int, *, status: str, results: list[dict[str, Any]] | None) -> None:
        values: dict[str, Any] = {"status": status}
        if results is not None:
            values["results_json"] = json.dumps(results, ensure_ascii=False)
            values["completed_at"] = db.now()
        db.update("forecast_sample_runs", values, "id=?", [run_id])

    @staticmethod
    def _latest_interrupted_run(task_id: int) -> dict[str, Any] | None:
        """只有最近一轮仍处于中断状态时才允许续做。"""
        return ForecastModelService._interrupted_run(task_id, None)

    @staticmethod
    def _interrupted_run(task_id: int, run_id: int | None) -> dict[str, Any] | None:
        """取指定的中断轮次；不指定时只看最近一轮。"""
        if run_id is not None:
            row = db.fetchone(
                "SELECT * FROM forecast_sample_runs WHERE id=? AND task_id=?",
                [run_id, task_id],
            )
        else:
            row = db.fetchone(
                "SELECT * FROM forecast_sample_runs WHERE task_id=? ORDER BY run_no DESC, id DESC LIMIT 1",
                [task_id],
            )
        if row is None or row["status"] not in INTERRUPTED_STATUSES:
            return None
        return row

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_tasks"] = db.count("forecast_tasks")
        snapshot["total_forecasts"] = db.count("forecast_results")
        snapshot["total_sample_runs"] = db.count("forecast_sample_runs")
        return snapshot


def _run_forecast_pipeline(task: dict[str, Any], sample_ids: list[int]) -> list[dict[str, Any]]:
    """调用预测算法，按需采用运行期替换过的函数（便于模拟取数后中断）。"""
    local = globals().get("compute_forecast", _IMPORTED_COMPUTE_FORECAST)
    if local is not _IMPORTED_COMPUTE_FORECAST:
        return local(dict(task), sample_ids=sample_ids)
    return pipeline.compute_forecast(dict(task), sample_ids=sample_ids)


def _load_ids(raw: Any) -> list[int]:
    try:
        values = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []
    return [int(value) for value in values or []]


def _load_json(raw: Any, fallback: Any) -> Any:
    try:
        return json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return fallback


def _run_summary(run_id: int) -> dict[str, Any]:
    row = db.fetchone("SELECT * FROM forecast_sample_runs WHERE id=?", [run_id])
    if row is None:
        return {"run_id": run_id}
    return {
        "run_id": row["id"],
        "run_no": row["run_no"],
        "sample_ids": _load_json(row["sample_ids"], []),
        "sample_values": _load_json(row["sample_values"], []),
        "calculation_order": _load_json(row["calculation_order"], []),
    }


def _run_record(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_id": row["id"],
        "task_id": row["task_id"],
        "run_no": row["run_no"],
        "metric_id": row["metric_id"],
        "status": row["status"],
        "method": row["method"],
        "source": row["source"],
        "sample_ids": _load_json(row["sample_ids"], []),
        "sample_values": _load_json(row["sample_values"], []),
        "sample_times": _load_json(row["sample_times"], []),
        "calculation_order": _load_json(row["calculation_order"], []),
        "results": _load_json(row["results_json"], []),
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
    }
