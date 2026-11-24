"""指标快照取样：趋势查询和预测模型共用的窗口读取与并列时间排序。

趋势和预测都必须按同一套规则从 ``metric_values`` 里取窗口样本：

* 窗口大小由调用方给出的 ``limit`` 决定（趋势默认 30 行，预测沿用任务的
  历史基线天数），本模块不改写任何调用方的行数上限。
* 展示方向保持“最新在前”：``snapshot_time DESC``。
* 同一时间戳的不同快照属于不同实际样本，必须用可复查的 ``id`` 打破并列，
  固定按 ``id DESC`` 排序；反转后即为计算方向（最旧在前、同时间戳 ``id`` 升序）。

把这两件事放在这里，指标存储的趋势查询和预测模型就不会各自维护一份
``ORDER BY``，也不会在同时间戳样本上出现不一致的取舍。
"""
from __future__ import annotations

from typing import Any, Iterable, Sequence

from app.database import db

#: 趋势查询的默认行数上限（沿用既有行为）。
TREND_LIMIT = 30
#: 预测的行窗口：在任务历史基线天数之内，最多取五行。
FORECAST_ROW_WINDOW = 5
#: 预测算法要求的最小样本量（沿用既有行为，不足时返回空结果）。
FORECAST_MIN_SAMPLES = 5

#: 窗口读取的唯一排序：最新在前，同一采样时间用编号倒序打破并列。
_WINDOW_ORDER = "ORDER BY snapshot_time DESC, id DESC"


def forecast_window_size(baseline_days: int) -> int:
    """预测的行窗口：五，且不超过任务配置的历史基线天数。

    历史基线天数仍是上界，所以行数上限不会被放大；不足最小样本量时
    仍按既有规则返回空结果。
    """
    return max(0, min(int(baseline_days), FORECAST_ROW_WINDOW))


def fetch_metric_snapshot_window(metric_id: int, limit: int = TREND_LIMIT) -> list[dict[str, Any]]:
    """读取指标最近 ``limit`` 条快照，按展示方向返回（最新在前）。

    返回顺序是总序：``snapshot_time`` 倒序，同一时间戳内 ``id`` 倒序。
    趋势查询直接消费这个顺序；预测模型需要用 ``computation_order`` 反转为
    计算方向后再运算。
    """
    return db.fetchall(
        f"SELECT * FROM metric_values WHERE metric_id=? {_WINDOW_ORDER} LIMIT ?",
        [metric_id, limit],
    )


def computation_order(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """把展示方向的窗口样本反转为计算方向（最旧在前）。

    因为展示方向是 ``snapshot_time DESC, id DESC``，反转后自然得到
    ``snapshot_time ASC, id ASC``：同一时间戳的样本按编号升序参与运算，
    顺序可复查、可复现。
    """
    return list(reversed(rows))


def sample_metric_snapshots(metric_id: int, limit: int = TREND_LIMIT) -> list[dict[str, Any]]:
    """按预测/趋势共用的窗口规则取样，返回计算方向（最旧在前）的样本。"""
    return computation_order(fetch_metric_snapshot_window(metric_id, limit))


def fetch_snapshot_sample_by_ids(metric_id: int, sample_ids: Iterable[int]) -> list[dict[str, Any]]:
    """按给定编号读取样本，并严格保持给定顺序。

    断点续做时用第一次落库的样本编号复核已选样本。这里只返回仍然存在的行，
    调用方需要自行校验数量；不会把新到的同时间戳快照拼进旧序列。
    """
    ids = [int(sample_id) for sample_id in sample_ids]
    if not ids:
        return []

    placeholders = ", ".join("?" for _ in ids)
    rows = db.fetchall(
        f"SELECT * FROM metric_values WHERE metric_id=? AND id IN ({placeholders})",
        [metric_id, *ids],
    )
    by_id = {int(row["id"]): row for row in rows}
    return [by_id[sample_id] for sample_id in ids if sample_id in by_id]


def snapshot_credential(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """把一组计算方向样本整理成可复查的取样凭据。

    ``sample_ids`` 与 ``sample_values`` / ``sample_times`` 一一对应，顺序就是
    预测真正采用的计算顺序；``calculation_order`` 给每一步附上编号和值，
    方便在结果复查时逐行核对。
    """
    ordered = list(rows)
    return {
        "sample_ids": [int(row["id"]) for row in ordered],
        "sample_values": [row["snapshot_value"] for row in ordered],
        "sample_times": [row["snapshot_time"] for row in ordered],
        "calculation_order": [
            {
                "step": step,
                "id": int(row["id"]),
                "snapshot_time": row["snapshot_time"],
                "snapshot_value": row["snapshot_value"],
            }
            for step, row in enumerate(ordered)
        ],
        "sample_count": len(ordered),
    }
