from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import date, datetime
import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd


LOG_LEVELS = {
    "NOTSET": logging.NOTSET,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


TRADE_JOURNAL_EVENT_TYPES = (
    "trade_opened",
    "trade_updated",
    "trade_closed",
    "order_submitted",
    "order_acknowledged",
    "order_filled",
    "order_cancelled",
    "order_rejected",
)


DAILY_DETAIL_COLUMNS = [
    # Session
    "ts",          # Tick timestamp in UTC.
    "date",        # Trading date for the session.
    "variant_id",  # Stable identifier for the sweep variant.
    "und_px",      # Underlying index price.
    "state",       # Session state after this tick.
    "struct",      # Active structure name.
    "qty",         # Open position quantity.
    "pos_evt",     # Position event on this tick.
    "exit_rsn",    # Encoded exit reason.
    "mark_px",     # Current structure mark/debit.
    "entry_px",    # Entry credit/price.
    "exit_px",     # Current executable exit debit/price.
    "u_pnl",       # Unrealized PnL in USD.
    "r_pnl",       # Realized PnL in USD.
    "t_pnl",       # Total PnL in USD.

    # Broker activity
    "open_ords",   # Open broker order count.
    "fills",       # Fill count on this tick.

    # Quote quality
    "opt_q",       # Requested-structure option quote count.
    "opt_q_all",   # All option quotes available in the snapshot.
    "q_live",      # Live quote count.
    "q_ffill",     # Forward-filled quote count.
    "q_untrust",   # Untrusted quote count.
    "q_miss",      # Missing quote count.

    # Phase 1
    "sl_p1",       # Phase 1 stop loss in USD.
    "tp_p1",       # Phase 1 take profit in USD.
    "act_p1",      # Phase 1 trailing activation in USD.
    "tr_dist_p1",  # Phase 1 trailing distance in USD.

    # Phase 2
    "sl_p2",       # Phase 2 stop loss in USD.
    "tp_p2",       # Phase 2 take profit in USD.
    "tr_on_p2",    # Trailing already active when phase 2 started.
    "act_p2",      # Phase 2 trailing activation in USD.
    "tr_dist_p2",  # Phase 2 trailing distance in USD.

    # Phase 3
    "sl_p3",       # Phase 3 stop loss in USD.
    "tp_p3",       # Phase 3 take profit in USD.
    "tr_on_p3",    # Trailing already active when phase 3 started.
    "act_p3",      # Phase 3 trailing activation in USD.
    "tr_dist_p3",  # Phase 3 trailing distance in USD.

    # Live runtime snapshot
    "mark_pts",    # Current structure mark in points.
    "be_lo",       # Lower break-even level.
    "be_hi",       # Upper break-even level.
    "ph_n",        # Active phase number.
    "sl_base",     # Current base stop loss in USD.
    "sl_eff",      # Current effective stop loss in USD.
    "tp_live",     # Current live take profit in USD.
    "tr_dist",     # Current live trailing distance in USD.
    "tr_on",       # Current live trailing active flag.

    # Leg snapshot
    "sc_k",        # Short call strike.
    "sc_m",        # Short call mid.
    "sp_k",        # Short put strike.
    "sp_m",        # Short put mid.
    "lc_k",        # Long call strike.
    "lc_m",        # Long call mid.
    "lp_k",        # Long put strike.
    "lp_m",        # Long put mid.

    # Cumulative PnL
    "cum_b",       # Gross cumulative PnL in USD.
    "cum_n",       # Net cumulative PnL in USD.

    # Tick note
    "note",        # Tick note or transition marker.
]


DAILY_SUMMARY_COLUMNS = [
    # Day summary
    "date",        # Trading date for the day summary.
    "variant_id",  # Stable identifier for the sweep variant.
    "wd",          # Weekday name.
    "ent_ts",      # First detected entry timestamp.
    "ex_ts",       # Final detected exit timestamp.
    "ent_px",      # First detected entry price.
    "ex_px",       # Last detected exit price.
    "r_pnl",       # Final realized PnL in USD.
    "min_t_pnl",   # Minimum total PnL in USD.
    "max_t_pnl",   # Maximum total PnL in USD.
    "exit_rsn",    # Final exit reason.
    "state_end",   # Final state of the day.
    "ticks",       # Number of recorded ticks.
    "fills",       # Total fill count.

    # Quote quality
    "pct_q_ok",    # Percent of trusted quotes.
    "q_live",      # Live quote count.
    "q_ffill",     # Forward-filled quote count.
    "q_untrust",   # Untrusted quote count.
    "q_miss",      # Missing quote count.
    "valid",       # True when no untrusted or missing quotes occurred.
    "pct_trusted_rows_window",   # Percent of trusted rows inside the evaluated window.
    "max_untrusted_gap_seconds", # Longest consecutive untrusted/missing gap in seconds.

    # Live runtime snapshot
    "ph_n",        # Last active phase number.
    "mark_pts",    # Last structure mark in points.
    "be_lo",       # Last lower break-even level.
    "be_hi",       # Last upper break-even level.
    "sl_base",     # Last base stop loss in USD.
    "sl_eff",      # Last effective stop loss in USD.
    "tp_live",     # Last live take profit in USD.
    "tr_dist",     # Last live trailing distance in USD.
    "tr_on",       # Last live trailing active flag.

    # Phase 1
    "sl_p1",       # Phase 1 stop loss in USD.
    "tp_p1",       # Phase 1 take profit in USD.
    "act_p1",      # Phase 1 trailing activation in USD.
    "tr_dist_p1",  # Phase 1 trailing distance in USD.

    # Phase 2
    "sl_p2",       # Phase 2 stop loss in USD.
    "tp_p2",       # Phase 2 take profit in USD.
    "tr_on_p2",    # Trailing already active when phase 2 started.
    "act_p2",      # Phase 2 trailing activation in USD.
    "tr_dist_p2",  # Phase 2 trailing distance in USD.

    # Phase 3
    "sl_p3",       # Phase 3 stop loss in USD.
    "tp_p3",       # Phase 3 take profit in USD.
    "tr_on_p3",    # Trailing already active when phase 3 started.
    "act_p3",      # Phase 3 trailing activation in USD.
    "tr_dist_p3",  # Phase 3 trailing distance in USD.
]


RUN_SUMMARY_COLUMNS = [
    # Dimensions
    "run_id",            # Run identifier.
    "variant_id",        # Stable identifier for the sweep variant.
    "validity_bucket",   # valid_only | all_days.

    # Sample size
    "num_days",          # Number of daily summary rows in this subset.
    "win_days",          # Count of positive-PnL days.
    "loss_days",         # Count of negative-PnL days.
    "flat_days",         # Count of zero/empty-PnL days.

    # PnL distribution
    "total_pnl",         # Sum of realized daily PnL.
    "mean_pnl",          # Mean realized daily PnL.
    "median_pnl",        # Median realized daily PnL.
    "std_pnl",           # Population standard deviation of realized daily PnL.
    "min_pnl",           # Minimum realized daily PnL.
    "max_pnl",           # Maximum realized daily PnL.
    "p50_pnl",           # 50th percentile of realized daily PnL.
    "p95_pnl",           # 95th percentile of realized daily PnL.

    # Win/loss stats
    "avg_win_pnl",       # Mean positive realized daily PnL.
    "avg_loss_pnl",      # Mean negative realized daily PnL.
    "max_day_gain",      # Best realized daily PnL.
    "max_day_loss",      # Worst realized daily PnL.
    "win_rate",          # win_days / num_days.
    "profit_factor",     # Gross wins / abs(gross losses).
    "payoff_ratio",      # Mean win / abs(mean loss).

    # Exit counts
    "num_sl1_exit",      # Count of sl1 day exits.
    "num_sl2_exit",      # Count of sl2 day exits.
    "num_sl3_exit",      # Count of sl3 day exits.
    "num_ts1_exit",      # Count of ts1 day exits.
    "num_ts2_exit",      # Count of ts2 day exits.
    "num_ts3_exit",      # Count of ts3 day exits.
    "num_tp1_exit",      # Count of tp1 day exits.
    "num_tp2_exit",      # Count of tp2 day exits.
    "num_tp3_exit",      # Count of tp3 day exits.
    "num_time_exit",     # Count of time day exits.

    # Exit rates
    "pct_exit_sl",       # Fraction of stop-loss exits among all exits.
    "pct_exit_ts",       # Fraction of trailing-stop exits among all exits.
    "pct_exit_tp",       # Fraction of take-profit exits among all exits.
    "pct_exit_time",     # Fraction of time exits among all exits.

    # Risk stats
    "avg_intraday_drawdown",  # Mean minimum intraday total PnL.
    "max_intraday_drawdown",  # Worst minimum intraday total PnL.
    "worst_exit_pnl",         # Worst realized daily PnL.
]


EXCEL_CSV_SEPARATOR = ";"
EXCEL_CSV_DECIMAL = ","


def _tick_time_hms(timestamp_value: Any) -> str:
    if isinstance(timestamp_value, datetime):
        return timestamp_value.strftime("%H:%M:%S")
    try:
        return datetime.fromisoformat(str(timestamp_value)).strftime("%H:%M:%S")
    except Exception:
        text = str(timestamp_value)
        if "T" in text:
            text = text.split("T", 1)[1]
        return text[:8]


def _format_tick_number(value: Any, digits: int = 2, *, na_text: str = "n/a") -> str:
    if value is None:
        return na_text
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return na_text


def _fmt_signed_tick(value: Any, width: int, digits: int) -> str:
    if value is None:
        return "-" * width
    try:
        return f"{float(value):+0{width}.{digits}f}"
    except Exception:
        return "-" * width


def _fmt_mark_tick(value: Any) -> str:
    if value is None:
        return "-------"
    try:
        return f"{float(value):6.3f}"
    except Exception:
        return "-------"


def _fmt_break_even_tick(lower_value: Any, upper_value: Any) -> str:
    if lower_value is None or upper_value is None:
        return "----|----"
    try:
        lower = int(round(float(lower_value)))
        upper = int(round(float(upper_value)))
        return f"{lower:04d}|{upper:04d}"
    except Exception:
        return "----|----"


def _fmt_phase_tick(value: Any) -> str:
    if value is None:
        return "-"
    try:
        return str(int(value))
    except Exception:
        return "-"


def _fmt_threshold_tick(value: Any) -> str:
    if value is None:
        return "----"
    try:
        return f"{int(round(float(value))):+4d}"
    except Exception:
        return "----"


def _fmt_trailing_tick(value: Any, *, active: bool) -> str:
    if value is None:
        distance = "---"
    else:
        try:
            distance = f"{int(round(float(value))):>3d}"
        except Exception:
            distance = "---"
    state = "on " if active else "off"
    return f"{state}{distance}"


def _format_leg_slot(prefix: str, strike: Any, mid: Any) -> str:
    strike_text = "----"
    if strike is not None:
        try:
            strike_text = f"{int(round(float(strike))):04d}"
        except Exception:
            strike_text = "----"
    mid_text = "-----"
    if mid is not None:
        try:
            mid_text = f"{float(mid):.2f}"
        except Exception:
            mid_text = "-----"
    return f"{prefix}{strike_text} m{mid_text}"


def build_live_tick_line_from_detail(row: dict[str, Any]) -> str:
    is_idle = str(row.get("struct") or "").strip().lower() == "flat" and int(row.get("qty") or 0) == 0
    if is_idle:
        return (
            f"[T] {_tick_time_hms(row.get('ts'))} | "
            f"SPX {_format_tick_number(row.get('und_px'), 2):>7} | "
            f"C---- m----- | P---- m----- | C---- m----- | P---- m----- | "
            f"M ------- | BE ----|---- | PH - | SLb ----- | SLe ----- | TP ----- | TR ---- | "
            f"U ------- | R ------- | N ------- | B -------"
        )

    legs = [
        _format_leg_slot("C", row.get("sc_k"), row.get("sc_m")),
        _format_leg_slot("P", row.get("sp_k"), row.get("sp_m")),
        _format_leg_slot("C", row.get("lc_k"), row.get("lc_m")),
        _format_leg_slot("P", row.get("lp_k"), row.get("lp_m")),
    ]
    return (
        f"[T] {_tick_time_hms(row.get('ts'))} | "
        f"SPX {_format_tick_number(row.get('und_px'), 2):>7} | "
        f"{legs[0]:<12} | {legs[1]:<12} | {legs[2]:<12} | {legs[3]:<12} | "
        f"M {_fmt_mark_tick(row.get('mark_pts'))} | "
        f"BE {_fmt_break_even_tick(row.get('be_lo'), row.get('be_hi'))} | "
        f"PH {_fmt_phase_tick(row.get('ph_n'))} | "
        f"SLb {_fmt_threshold_tick(row.get('sl_base'))} | "
        f"SLe {_fmt_threshold_tick(row.get('sl_eff'))} | "
        f"TP {_fmt_threshold_tick(row.get('tp_live'))} | "
        f"TR {_fmt_trailing_tick(row.get('tr_dist'), active=bool(row.get('tr_on')))} | "
        f"U {_fmt_signed_tick(row.get('u_pnl'), 7, 2)} | "
        f"R {_fmt_signed_tick(row.get('r_pnl'), 7, 2)} | "
        f"N {_fmt_signed_tick(row.get('cum_n'), 7, 2)} | "
        f"B {_fmt_signed_tick(row.get('cum_b'), 7, 2)}"
    )


def resolve_excel_export_dir_for_run(run_dir: str | Path, *, project_root: str | Path) -> Path:
    resolved_run_dir = Path(run_dir).resolve()
    resolved_project_root = Path(project_root).resolve()

    try:
        relative_run_dir = resolved_run_dir.relative_to(resolved_project_root)
    except ValueError as exc:
        raise ValueError(f"Run directory '{resolved_run_dir}' is not inside project root '{resolved_project_root}'") from exc

    parts = relative_run_dir.parts
    if len(parts) >= 5 and parts[:4] == ("data", "backtests", "replay", "mybot3"):
        return resolved_project_root / "data" / "exports" / "excel" / "backtests" / "replay" / "mybot3" / parts[4]
    if len(parts) >= 4 and parts[:3] == ("data", "performance", "paper"):
        return resolved_project_root / "data" / "exports" / "excel" / "performance" / "paper" / parts[3]
    if len(parts) >= 4 and parts[:3] == ("data", "performance", "live"):
        return resolved_project_root / "data" / "exports" / "excel" / "performance" / "live" / parts[3]

    raise ValueError(
        "Unsupported run directory for Excel CSV export. "
        f"Expected data/backtests/replay/mybot3/<run_id>, data/performance/paper/<run_id>, or data/performance/live/<run_id>; got '{relative_run_dir}'"
    )


def _write_excel_csv(frame: pd.DataFrame, *, target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(target, index=False, sep=EXCEL_CSV_SEPARATOR, decimal=EXCEL_CSV_DECIMAL, encoding="utf-8")
    return target


def _read_export_frame(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    if path.suffix == ".jsonl":
        if path.stat().st_size == 0:
            return pd.DataFrame()
        return pd.read_json(path, lines=True)
    raise ValueError(f"Unsupported export source file '{path}'")


def _iter_export_source_files(run_dir: Path) -> list[Path]:
    source_files = sorted(path for path in run_dir.glob("*.parquet") if path.is_file())
    trade_journal_path = run_dir / "trade_journal.jsonl"
    if trade_journal_path.is_file():
        source_files.append(trade_journal_path)
    return source_files


def export_run_to_excel_csv(run_dir: str | Path, *, project_root: str | Path) -> Path:
    resolved_run_dir = Path(run_dir).resolve()
    export_dir = resolve_excel_export_dir_for_run(resolved_run_dir, project_root=project_root)
    source_files = _iter_export_source_files(resolved_run_dir)
    if not source_files:
        raise ValueError(f"No exportable files found in run directory '{resolved_run_dir}'")

    for source_path in source_files:
        frame = _read_export_frame(source_path)
        _write_excel_csv(
            frame,
            target=export_dir / f"{source_path.stem}_excel.csv",
        )

    return export_dir


@dataclass(frozen=True)
class DailyDetailRow:
    # Session
    ts: datetime
    date: str | date
    und_px: float | None
    state: str
    struct: str
    qty: int
    pos_evt: str
    exit_rsn: str | None
    mark_px: float | None
    entry_px: float | None
    exit_px: float | None
    u_pnl: float | None
    r_pnl: float | None
    t_pnl: float | None

    # Broker activity
    open_ords: int
    fills: int

    # Quote quality
    opt_q: int = 0
    opt_q_all: int = 0
    q_live: int = 0
    q_ffill: int = 0
    q_untrust: int = 0
    q_miss: int = 0

    # Phase 1
    sl_p1: float | None = None
    tp_p1: float | None = None
    act_p1: float | None = None
    tr_dist_p1: float | None = None

    # Phase 2
    sl_p2: float | None = None
    tp_p2: float | None = None
    tr_on_p2: bool | None = None
    act_p2: float | None = None
    tr_dist_p2: float | None = None

    # Phase 3
    sl_p3: float | None = None
    tp_p3: float | None = None
    tr_on_p3: bool | None = None
    act_p3: float | None = None
    tr_dist_p3: float | None = None

    # Live runtime snapshot
    mark_pts: float | None = None
    be_lo: float | None = None
    be_hi: float | None = None
    ph_n: int | None = None
    sl_base: float | None = None
    sl_eff: float | None = None
    tp_live: float | None = None
    tr_dist: float | None = None
    tr_on: bool | None = None

    # Leg snapshot
    sc_k: float | None = None
    sc_m: float | None = None
    sp_k: float | None = None
    sp_m: float | None = None
    lc_k: float | None = None
    lc_m: float | None = None
    lp_k: float | None = None
    lp_m: float | None = None

    # Cumulative PnL
    cum_b: float | None = None
    cum_n: float | None = None

    # Tick note
    note: str | None = None


class TradingRunOutput:
    def __init__(
        self,
        base_dir: str | Path = "data/mybot3",
        run_id: str | None = None,
        *,
        write_market: bool = False,
        log_level: str = "INFO",
        valid_day_policy: dict[str, float | int] | None = None,
        emit_live_tick_lines: bool = False,
    ):
        self.base_dir = Path(base_dir)
        self.run_id = run_id or f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_m3"
        self.run_dir = self.base_dir / self.run_id
        self.market_dir = self.run_dir / "market"
        self.log_file = self.run_dir / "run.log"
        self.run_metadata_file = self.run_dir / "run_metadata.json"
        self.run_result_file = self.run_dir / "run_result.json"
        self.run_results_file = self.run_dir / "run_results.txt"
        self.trade_journal_file = self.run_dir / "trade_journal.jsonl"
        self.write_market = write_market
        self.emit_live_tick_lines = bool(emit_live_tick_lines)
        default_valid_day_policy = {
            "max_gap_seconds_not_trusted": 0.0,
            "pct_trusted_rows_in_window": 100.0,
        }
        self.valid_day_policy = {
            **default_valid_day_policy,
            **(valid_day_policy or {}),
        }
        self.log_level_name = str(log_level).upper()
        if self.log_level_name not in LOG_LEVELS:
            raise ValueError(f"Unsupported log level '{log_level}'")
        self.log_level = LOG_LEVELS[self.log_level_name]
        self._market_index_rows: dict[str, list[dict[str, Any]]] = {}
        self._market_option_rows: dict[str, list[dict[str, Any]]] = {}
        self._market_index_names: dict[str, str] = {}
        self._market_option_names: dict[str, str] = {}
        self._session_actions: list[dict[str, Any]] = []
        self._instrument_fill_stats: dict[str, dict[str, Any]] = {}
        self._daily_detail_rows: list[dict[str, Any]] = []
        self._trade_journal_rows: list[dict[str, Any]] = []
        self._order_trade_ids: dict[str, str] = {}
        self._seen_fill_keys: set[tuple[Any, ...]] = set()
        self._seen_order_status_keys: set[tuple[Any, ...]] = set()
        self._trade_journal_seq = 0
        self._variant_context: dict[str, str | float | None] = {
            "variant_id": "default",
        }

        self.run_dir.mkdir(parents=True, exist_ok=True)
        self._log_handle = self.log_file.open("a", encoding="utf-8")
        self._trade_journal_handle = self.trade_journal_file.open("a", encoding="utf-8")

    def set_variant_context(
        self,
        *,
        variant_id: str | None = None,
    ) -> None:
        self._variant_context = {
            "variant_id": variant_id or "default",
        }

    def log(self, level: str, message: str, *, timestamp: datetime | None = None, **fields: Any) -> None:
        if not self._should_log(level):
            return
        event_time = timestamp or datetime.now()
        rendered = self._format_log_line(event_time, level, message, self._contextualize_fields(fields))
        print(rendered)
        self._log_handle.write(rendered + "\n")

    def write_market_data(self, snapshot: Any) -> None:
        if self.write_market:
            self._buffer_processed_market_data(snapshot)

    def write_run_result(self, result: Any) -> None:
        payload = self._normalize_record(result)
        self.run_result_file.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def write_run_metadata(self, metadata: Any) -> None:
        payload = self._normalize_record(metadata)
        self.run_metadata_file.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")

    def record_session_action(
        self,
        *,
        timestamp: datetime | None,
        source: str,
        stage: str,
        event: str,
        **fields: Any,
    ) -> None:
        action_time = timestamp or datetime.now()
        self._session_actions.append(
            {
                "timestamp": action_time,
                "source": source,
                "stage": stage,
                "event": event,
                "fields": self._contextualize_fields(fields),
            }
        )

    def record_broker_snapshot(self, snapshot: Any) -> None:
        timestamp = getattr(snapshot, "timestamp", None)
        open_orders = getattr(snapshot, "open_orders", None)
        if isinstance(open_orders, list):
            for order in open_orders:
                self._record_broker_order(timestamp, order)
        fills = getattr(snapshot, "fills", None)
        if not isinstance(fills, list):
            return
        for fill in fills:
            self._record_broker_fill(timestamp, fill)

    def record_order_event(
        self,
        *,
        timestamp: datetime | None,
        event_type: str,
        order_id: str,
        trade_id: str | None = None,
        parent_order_id: str | None = None,
        source: str = "session",
        state: str | None = None,
        phase: str | None = None,
        structure: str | None = None,
        payload: dict[str, Any] | None = None,
        **fields: Any,
    ) -> None:
        normalized_payload = dict(payload or {})
        normalized_payload.update(fields)
        self._append_trade_journal_event(
            timestamp=timestamp,
            event_type=event_type,
            trade_id=trade_id,
            order_id=order_id,
            parent_order_id=parent_order_id,
            source=source,
            state=state,
            phase=phase,
            structure=structure,
            payload=normalized_payload,
        )

    def record_trade_event(
        self,
        *,
        timestamp: datetime | None,
        event_type: str,
        trade_id: str,
        order_id: str | None = None,
        source: str = "session",
        state: str | None = None,
        phase: str | None = None,
        structure: str | None = None,
        payload: dict[str, Any] | None = None,
        **fields: Any,
    ) -> None:
        normalized_payload = dict(payload or {})
        normalized_payload.update(fields)
        self._append_trade_journal_event(
            timestamp=timestamp,
            event_type=event_type,
            trade_id=trade_id,
            order_id=order_id,
            parent_order_id=None,
            source=source,
            state=state,
            phase=phase,
            structure=structure,
            payload=normalized_payload,
        )

    def record_daily_detail(self, row: DailyDetailRow | dict[str, Any]) -> None:
        normalized_row = self._normalize_record(row)
        if not isinstance(normalized_row, dict):
            raise ValueError("Daily detail row must normalize to a dictionary")
        normalized_row = self._contextualize_fields(normalized_row)
        trade_date = self._normalize_trade_date_key(
            normalized_row.get("date") or normalized_row.get("ts") or datetime.now().date().isoformat()
        )
        csv_row = {column: normalized_row.get(column) for column in DAILY_DETAIL_COLUMNS}
        csv_row["date"] = trade_date
        self._daily_detail_rows.append(csv_row)
        if self.emit_live_tick_lines:
            self._emit_live_tick_line(csv_row)

    def write_run_results(self) -> None:
        self.run_results_file.write_text(self._render_run_results(), encoding="utf-8")

    def write_daily_details(self) -> None:
        frame = pd.DataFrame(self._daily_detail_rows, columns=DAILY_DETAIL_COLUMNS)
        frame.to_parquet(self.daily_details_file(), index=False)

    def write_daily_summary(self) -> None:
        frame = pd.DataFrame(self._render_daily_summary_rows(), columns=DAILY_SUMMARY_COLUMNS)
        frame.to_parquet(self.daily_summary_file(), index=False)

    def write_run_summary(self) -> None:
        frame = pd.DataFrame(self._render_run_summary_rows(), columns=RUN_SUMMARY_COLUMNS)
        frame.to_parquet(self.run_summary_file(), index=False)

    def daily_details_file(self) -> Path:
        return self.run_dir / "daily_details.parquet"

    def daily_summary_file(self) -> Path:
        return self.run_dir / "daily_summary.parquet"

    def run_summary_file(self) -> Path:
        return self.run_dir / "run_summary.parquet"

    def flush(self) -> None:
        self._log_handle.flush()
        self._trade_journal_handle.flush()

    def close(self) -> None:
        if self.write_market:
            self._flush_processed_market_data()
        self.write_daily_details()
        self.write_daily_summary()
        self.write_run_summary()
        self.write_run_results()
        self.flush()
        self._log_handle.close()
        self._trade_journal_handle.close()

    def export_files_to_excel_csv(self, *, project_root: str | Path) -> Path:
        return export_run_to_excel_csv(self.run_dir, project_root=project_root)

    def _buffer_processed_market_data(self, snapshot: Any) -> None:
        market_snapshot = getattr(snapshot, "market_snapshot", snapshot)
        timestamp = getattr(market_snapshot, "timestamp", None)
        metadata = getattr(market_snapshot, "metadata", {}) or {}
        if timestamp is None or not isinstance(metadata, dict):
            return

        trade_date = str(metadata.get("trade_date") or timestamp.date().isoformat())
        index_row = metadata.get("index_row")
        if isinstance(index_row, dict):
            normalized_index_row = {"timestamp": timestamp, **index_row}
            self._market_index_rows.setdefault(trade_date, []).append(normalized_index_row)
            self._market_index_names.setdefault(
                trade_date,
                self._resolve_market_file_name(trade_date, metadata.get("index_file"), default_stem="spx_ohlcv_5s"),
            )

        option_quotes = metadata.get("option_quotes")
        if isinstance(option_quotes, list):
            normalized_option_rows = []
            for quote in option_quotes:
                if not isinstance(quote, dict):
                    continue
                normalized_option_rows.append({"timestamp": timestamp, **quote})
            if normalized_option_rows:
                self._market_option_rows.setdefault(trade_date, []).extend(normalized_option_rows)
                self._market_option_names.setdefault(
                    trade_date,
                    self._resolve_market_file_name(trade_date, metadata.get("options_file"), default_stem="spxw_quotes_5s"),
                )

    def _flush_processed_market_data(self) -> None:
        if not self._market_index_rows and not self._market_option_rows:
            return

        self.market_dir.mkdir(parents=True, exist_ok=True)

        for trade_date, rows in self._market_index_rows.items():
            if not rows:
                continue
            path = self.market_dir / self._market_index_names[trade_date]
            self._write_parquet_rows(path, rows)

        for trade_date, rows in self._market_option_rows.items():
            if not rows:
                continue
            path = self.market_dir / self._market_option_names[trade_date]
            self._write_parquet_rows(path, rows)

    def _write_parquet_rows(self, path: Path, rows: list[dict[str, Any]]) -> None:
        frame = pd.DataFrame([self._normalize_record(row) for row in rows])
        frame.to_parquet(path, index=False)

    def _emit_live_tick_line(self, row: dict[str, Any]) -> None:
        line = build_live_tick_line_from_detail(row)
        print(line)
        self._log_handle.write(line + "\n")

    def _resolve_market_file_name(self, trade_date: str, source_path: Any, *, default_stem: str) -> str:
        if isinstance(source_path, str) and source_path:
            return Path(source_path).name
        ymd = trade_date.replace("-", "")
        return f"{default_stem}_{ymd}.processed.parquet"

    def _format_log_line(
        self,
        timestamp: datetime,
        level: str,
        message: str,
        fields: dict[str, Any],
    ) -> str:
        prefix = f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} [{level.upper()}] {message}"
        if not fields:
            return prefix
        field_text = " ".join(f"{key}={self._format_field_value(value)}" for key, value in fields.items())
        return f"{prefix} {field_text}"

    def _should_log(self, level: str) -> bool:
        normalized = str(level).upper()
        if normalized not in LOG_LEVELS:
            raise ValueError(f"Unsupported log level '{level}'")
        return LOG_LEVELS[normalized] >= self.log_level

    def _normalize_record(self, record: Any) -> Any:
        if isinstance(record, (str, int, float, bool)) or record is None:
            return record
        if isinstance(record, datetime):
            return record.isoformat()
        if isinstance(record, date):
            return record.isoformat()
        if isinstance(record, Path):
            return str(record)
        if is_dataclass(record):
            return self._normalize_record(asdict(record))
        if isinstance(record, dict):
            return {key: self._normalize_record(value) for key, value in record.items()}
        if isinstance(record, (list, tuple)):
            return [self._normalize_record(value) for value in record]
        if hasattr(record, "model_dump"):
            return self._normalize_record(record.model_dump())
        if hasattr(record, "__dict__"):
            return self._normalize_record(vars(record))
        return {"value": record}

    def _format_field_value(self, value: Any) -> str:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return str(value)
        return json.dumps(self._normalize_record(value), ensure_ascii=True, sort_keys=True)

    def _append_trade_journal_event(
        self,
        *,
        timestamp: datetime | None,
        event_type: str,
        trade_id: str | None,
        order_id: str | None,
        parent_order_id: str | None,
        source: str,
        state: str | None,
        phase: str | None,
        structure: str | None,
        payload: dict[str, Any] | None,
    ) -> None:
        if event_type not in TRADE_JOURNAL_EVENT_TYPES:
            raise ValueError(f"Unsupported trade journal event type '{event_type}'")
        event_time = timestamp or datetime.now()
        normalized_payload = self._normalize_record(payload or {})
        if not isinstance(normalized_payload, dict):
            normalized_payload = {"value": normalized_payload}
        trade_date = self._normalize_trade_date_key(
            normalized_payload.get("trade_date") or event_time.date().isoformat()
        )
        self._trade_journal_seq += 1
        entry = {
            "timestamp": self._normalize_record(event_time),
            "seq": self._trade_journal_seq,
            "run_id": self.run_id,
            "trade_date": trade_date,
            "variant_id": self._current_variant_fields().get("variant_id"),
            "event_type": event_type,
            "trade_id": trade_id,
            "order_id": order_id,
            "parent_order_id": parent_order_id,
            "source": source,
            "state": state,
            "phase": phase,
            "structure": structure,
            "payload": self._contextualize_fields(normalized_payload),
        }
        self._trade_journal_rows.append(entry)
        if trade_id and order_id:
            self._order_trade_ids[str(order_id)] = str(trade_id)
        if trade_id and parent_order_id:
            self._order_trade_ids.setdefault(str(parent_order_id), str(trade_id))
        self._trade_journal_handle.write(json.dumps(entry, ensure_ascii=True, sort_keys=True) + "\n")

    def _resolve_trade_id_for_order(
        self,
        *,
        order_id: Any,
        parent_order_id: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        if isinstance(metadata, dict):
            metadata_trade_id = metadata.get("trade_id")
            if metadata_trade_id:
                return str(metadata_trade_id)
        if order_id is not None:
            mapped = self._order_trade_ids.get(str(order_id))
            if mapped:
                return mapped
        if parent_order_id is not None:
            mapped = self._order_trade_ids.get(str(parent_order_id))
            if mapped:
                return mapped
        return None

    def _record_broker_order(self, timestamp: datetime | None, order: Any) -> None:
        metadata = getattr(order, "metadata", {}) or {}
        if not isinstance(metadata, dict):
            metadata = {}
        order_id = getattr(order, "order_id", None)
        if not order_id:
            return
        status = str(getattr(order, "status", "") or "").strip().lower()
        filled_quantity = getattr(order, "filled_quantity", None)
        remaining_quantity = getattr(order, "remaining_quantity", None)
        order_key = (order_id, status, filled_quantity, remaining_quantity)
        if order_key in self._seen_order_status_keys:
            return
        self._seen_order_status_keys.add(order_key)

        if status in {"rejected", "reject", "inactive"}:
            event_type = "order_rejected"
        elif status in {"cancelled", "canceled"}:
            event_type = "order_cancelled"
        else:
            event_type = "order_acknowledged"

        parent_order_id = metadata.get("parent_order_id")
        trade_id = self._resolve_trade_id_for_order(order_id=order_id, parent_order_id=parent_order_id, metadata=metadata)

        self.record_order_event(
            timestamp=timestamp,
            event_type=event_type,
            order_id=str(order_id),
            trade_id=trade_id,
            parent_order_id=parent_order_id,
            source="broker",
            state=metadata.get("state"),
            phase=metadata.get("phase"),
            structure=getattr(order, "structure_type", None),
            purpose=getattr(order, "purpose", None),
            status=getattr(order, "status", None),
            quantity=getattr(order, "quantity", None),
            filled_quantity=filled_quantity,
            remaining_quantity=remaining_quantity,
            metadata=metadata,
        )

    def _record_broker_fill(self, timestamp: datetime | None, fill: Any) -> None:
        metadata = getattr(fill, "metadata", {}) or {}
        if not isinstance(metadata, dict):
            metadata = {}

        order_id = getattr(fill, "order_id", None)
        quantity = getattr(fill, "quantity", None)
        fill_price = getattr(fill, "fill_price", None)
        side = str(metadata.get("side") or metadata.get("action") or "").upper()
        instrument = str(metadata.get("instrument") or metadata.get("symbol") or metadata.get("contract") or "UNKNOWN")
        fill_key = (
            timestamp.isoformat() if isinstance(timestamp, datetime) else None,
            order_id,
            getattr(fill, "purpose", None),
            getattr(fill, "status", None),
            quantity,
            fill_price,
            side,
            instrument,
        )
        if fill_key in self._seen_fill_keys:
            return
        self._seen_fill_keys.add(fill_key)

        parent_order_id = metadata.get("parent_order_id")
        trade_id = self._resolve_trade_id_for_order(order_id=order_id, parent_order_id=parent_order_id, metadata=metadata)

        self.record_order_event(
            timestamp=timestamp,
            event_type="order_filled",
            order_id=str(order_id) if order_id is not None else "",
            trade_id=trade_id,
            parent_order_id=parent_order_id,
            source="broker",
            state=metadata.get("state"),
            phase=metadata.get("phase"),
            structure=getattr(fill, "structure_type", None),
            purpose=getattr(fill, "purpose", None),
            status=getattr(fill, "status", None),
            side=side,
            instrument=instrument,
            quantity=quantity,
            fill_price=fill_price,
            metadata=metadata,
        )

        quantity_value = int(quantity) if isinstance(quantity, int) else int(quantity or 0)
        fill_price_value = float(fill_price) if isinstance(fill_price, (int, float)) else None
        commission = metadata.get("commission", 0.0)
        commission_value = float(commission) if isinstance(commission, (int, float)) else 0.0

        self.record_session_action(
            timestamp=timestamp,
            source="broker",
            stage="fill",
            event="broker_fill",
            order_id=order_id,
            purpose=getattr(fill, "purpose", None),
            status=getattr(fill, "status", None),
            side=side,
            instrument=instrument,
            quantity=quantity_value,
            fill_price=fill_price_value,
            commission=commission_value,
        )

        leg_fills = metadata.get("leg_fills")
        if isinstance(leg_fills, list) and leg_fills:
            for leg_fill in leg_fills:
                if isinstance(leg_fill, dict):
                    self._accumulate_fill_stats(timestamp=timestamp, metadata=metadata, fill_data=leg_fill)
            return

        self._accumulate_fill_stats(
            timestamp=timestamp,
            metadata=metadata,
            fill_data={
                "instrument": instrument,
                "side": side,
                "quantity": quantity_value,
                "fill_price": fill_price_value,
                "commission": commission_value,
                "multiplier": metadata.get("multiplier", 100),
            },
        )

    def _accumulate_fill_stats(
        self,
        *,
        timestamp: datetime | None,
        metadata: dict[str, Any],
        fill_data: dict[str, Any],
    ) -> None:
        instrument = str(fill_data.get("instrument") or metadata.get("instrument") or metadata.get("symbol") or metadata.get("contract") or "UNKNOWN")
        side = str(fill_data.get("side") or metadata.get("side") or metadata.get("action") or "").upper()
        quantity = fill_data.get("quantity", 0)
        quantity_value = int(quantity) if isinstance(quantity, int) else int(quantity or 0)
        fill_price = fill_data.get("fill_price")
        fill_price_value = float(fill_price) if isinstance(fill_price, (int, float)) else None
        multiplier = fill_data.get("multiplier", metadata.get("multiplier", 100))
        multiplier_value = float(multiplier) if isinstance(multiplier, (int, float)) else 100.0
        commission = fill_data.get("commission", 0.0)
        commission_value = float(commission) if isinstance(commission, (int, float)) else 0.0
        gross_value = (fill_price_value or 0.0) * quantity_value * multiplier_value

        stats = self._instrument_fill_stats.setdefault(
            (*self._variant_key_from_mapping(self._current_variant_fields()), instrument),
            {
                "instrument": instrument,
                "trade_date": self._normalize_trade_date_key(
                    metadata.get("trade_date") or (timestamp.date().isoformat() if isinstance(timestamp, datetime) else datetime.now().date().isoformat())
                ),
                "buys": 0,
                "sells": 0,
                "buy_quantity": 0,
                "sell_quantity": 0,
                "total_buy": 0.0,
                "total_sell": 0.0,
                "commission": 0.0,
            },
        )
        stats["commission"] += commission_value
        if side in {"BOT", "BUY", "B"}:
            stats["buys"] += quantity_value
            stats["buy_quantity"] += quantity_value
            stats["total_buy"] += gross_value
        elif side in {"SLD", "SELL", "S"}:
            stats["sells"] += quantity_value
            stats["sell_quantity"] += quantity_value
            stats["total_sell"] += gross_value

    def _render_run_results(self) -> str:
        rendered_sections = self._render_daily_result_sections()
        sections = ["Run results", f"Run log: {self.log_file}", ""]
        summary_section = self._render_run_summary_section()
        if summary_section:
            sections.extend(["Run summary:", *summary_section, ""])

        if not rendered_sections:
            sections.append("No daily results recorded.")
            sections.append("")
            return "\n".join(sections)

        sections.append("Daily results:")
        sections.append("")
        for index, rendered in enumerate(rendered_sections):
            if index > 0:
                sections.extend(["", "=" * 80, ""])
            sections.append(rendered.rstrip())
        sections.append("")
        return "\n".join(sections)

    def _render_daily_result_sections(self) -> list[str]:
        grouped_actions: dict[tuple[str, str], list[dict[str, Any]]] = {}
        grouped_fill_stats: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        grouped_trade_journal: dict[tuple[str, str], list[dict[str, Any]]] = {}

        for action in self._session_actions:
            trade_date = self._action_trade_date(action)
            variant_key = self._variant_key_from_mapping(action.get("fields", {}) or {})
            grouped_actions.setdefault((trade_date, *variant_key), []).append(action)

        for instrument_key, stats in self._instrument_fill_stats.items():
            trade_date = self._instrument_trade_date(stats)
            grouped_fill_stats.setdefault(
                (trade_date, str(stats.get("variant_id") or "default")),
                {},
            )[str(stats.get("instrument") or instrument_key[-1])] = stats

        for row in self._trade_journal_rows:
            trade_date = self._normalize_trade_date_key(row.get("trade_date") or datetime.now().date().isoformat())
            grouped_trade_journal.setdefault(
                (trade_date, str(row.get("variant_id") or "default")),
                [],
            ).append(row)

        detail_section_keys = {
            (
                self._normalize_trade_date_key(row.get("date") or datetime.now().date().isoformat()),
                *self._variant_key_from_mapping(row),
            )
            for row in self._daily_detail_rows
        }

        all_section_keys = sorted(
            set(grouped_actions) | set(grouped_fill_stats) | set(grouped_trade_journal) | detail_section_keys,
            key=lambda item: (item[0], str(item[1])),
        )
        if not all_section_keys:
            today = datetime.now().date().isoformat()
            all_section_keys = [(today, "default")]

        return [
            self._render_trading_session_summary(
                trade_date=trade_date,
                variant_id=variant_id,
                actions=grouped_actions.get((trade_date, variant_id), []),
                fill_stats=grouped_fill_stats.get((trade_date, variant_id), {}),
                trade_journal_rows=grouped_trade_journal.get((trade_date, variant_id), []),
            )
            for trade_date, variant_id in all_section_keys
        ]

    def _render_trading_session_summary(
        self,
        *,
        trade_date: str,
        variant_id: str,
        actions: list[dict[str, Any]],
        fill_stats: dict[str, dict[str, Any]],
        trade_journal_rows: list[dict[str, Any]],
    ) -> str:
        lines = [
            f"Variant: {self._variant_label(variant_id=variant_id)}",
            f"Trade date: {trade_date}",
            f"Run log: {self.log_file}",
            "",
            "Action timeline:",
        ]

        if actions:
            action_rows = []
            for action in sorted(actions, key=lambda item: item["timestamp"]):
                timestamp = action["timestamp"]
                fields = dict(action.get("fields", {}))
                detail = self._render_action_detail(fields)
                action_rows.append(
                    {
                        "time": timestamp.isoformat() if isinstance(timestamp, datetime) else str(timestamp),
                        "source": str(action["source"]),
                        "stage": str(action["stage"]),
                        "event": str(action["event"]),
                        "detail": detail,
                    }
                )
            lines.extend(
                self._render_text_table(
                    columns=["time", "source", "stage", "event", "detail"],
                    rows=action_rows,
                )
            )
        else:
            lines.append("  No action timeline events recorded.")

        lines.extend(["", "Instrument fills:"])

        if fill_stats:
            fill_rows = []
            total_buy_quantity = 0
            total_sell_quantity = 0
            total_buy_value = 0.0
            total_sell_value = 0.0
            total_commission = 0.0
            for instrument, stats in sorted(fill_stats.items()):
                buy_qty = int(stats["buy_quantity"])
                sell_qty = int(stats["sell_quantity"])
                avg_buy = stats["total_buy"] / buy_qty if buy_qty else 0.0
                avg_sell = stats["total_sell"] / sell_qty if sell_qty else 0.0
                net_total = stats["total_sell"] - stats["total_buy"]
                net_incl_commission = net_total - stats["commission"]
                total_buy_quantity += buy_qty
                total_sell_quantity += sell_qty
                total_buy_value += stats["total_buy"]
                total_sell_value += stats["total_sell"]
                total_commission += stats["commission"]
                fill_rows.append(
                    {
                        "instrument": instrument,
                        "buys": str(buy_qty),
                        "sells": str(sell_qty),
                        "net": str(sell_qty - buy_qty),
                        "avg_bot": f"{avg_buy:.2f}",
                        "avg_sld": f"{avg_sell:.2f}",
                        "total_bot": f"{stats['total_buy']:.2f}",
                        "total_sld": f"{stats['total_sell']:.2f}",
                        "net_total": f"{net_total:.2f}",
                        "commission": f"{stats['commission']:.2f}",
                        "net_incl_cmm": f"{net_incl_commission:.2f}",
                    }
                )
            total_net = total_sell_value - total_buy_value
            total_net_incl_commission = total_net - total_commission
            lines.extend(
                self._render_text_table(
                    columns=[
                        "instrument",
                        "buys",
                        "sells",
                        "net",
                        "avg_bot",
                        "avg_sld",
                        "total_bot",
                        "total_sld",
                        "net_total",
                        "commission",
                        "net_incl_cmm",
                    ],
                    rows=fill_rows,
                    totals={
                        "instrument": "TOTAL",
                        "buys": str(total_buy_quantity),
                        "sells": str(total_sell_quantity),
                        "net": str(total_sell_quantity - total_buy_quantity),
                        "avg_bot": f"{(total_buy_value / total_buy_quantity) if total_buy_quantity else 0.0:.2f}",
                        "avg_sld": f"{(total_sell_value / total_sell_quantity) if total_sell_quantity else 0.0:.2f}",
                        "total_bot": f"{total_buy_value:.2f}",
                        "total_sld": f"{total_sell_value:.2f}",
                        "net_total": f"{total_net:.2f}",
                        "commission": f"{total_commission:.2f}",
                        "net_incl_cmm": f"{total_net_incl_commission:.2f}",
                    },
                    right_align_columns={
                        "buys",
                        "sells",
                        "net",
                        "avg_bot",
                        "avg_sld",
                        "total_bot",
                        "total_sld",
                        "net_total",
                        "commission",
                        "net_incl_cmm",
                    },
                    header_separator=False,
                    totals_separator_style="full",
                )
            )
        else:
            lines.append("  No broker fills recorded.")

        lines.extend(["", "Trade journal summary:"])
        trade_rows = self._render_trade_journal_summary_rows(trade_journal_rows)
        if trade_rows:
            lines.extend(
                self._render_text_table(
                    columns=["trade_id", "opened_at", "closed_at", "structure", "status", "entry_order", "exit_order", "exit_reason", "realized_pnl_usd"],
                    rows=trade_rows,
                )
            )
        else:
            lines.append("  No trade journal events recorded.")

        return "\n".join(lines) + "\n"

    def _render_trade_journal_summary_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, str]]:
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            trade_id = row.get("trade_id")
            if not trade_id:
                continue
            grouped.setdefault(str(trade_id), []).append(row)

        rendered: list[dict[str, str]] = []
        for trade_id, trade_rows in sorted(grouped.items()):
            ordered = sorted(trade_rows, key=lambda item: int(item.get("seq") or 0))
            opened = next((row for row in ordered if row.get("event_type") == "trade_opened"), None)
            closed = next((row for row in reversed(ordered) if row.get("event_type") == "trade_closed"), None)
            latest = ordered[-1]
            opened_payload = opened.get("payload", {}) if isinstance(opened, dict) else {}
            closed_payload = closed.get("payload", {}) if isinstance(closed, dict) else {}
            if not isinstance(opened_payload, dict):
                opened_payload = {}
            if not isinstance(closed_payload, dict):
                closed_payload = {}
            rendered.append(
                {
                    "trade_id": trade_id,
                    "opened_at": str(opened.get("timestamp") if opened else ""),
                    "closed_at": str(closed.get("timestamp") if closed else ""),
                    "structure": str((closed or opened or latest).get("structure") or ""),
                    "status": "closed" if closed is not None else "open",
                    "entry_order": str(opened.get("order_id") if opened else ""),
                    "exit_order": str(closed.get("order_id") if closed else ""),
                    "exit_reason": str(closed_payload.get("exit_reason") or ""),
                    "realized_pnl_usd": str(closed_payload.get("realized_pnl_usd") or ""),
                }
            )
        return rendered

    def _render_daily_summary_rows(self) -> list[dict[str, Any]]:
        if not self._daily_detail_rows:
            return []

        grouped_rows: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in self._daily_detail_rows:
            trade_date = self._normalize_trade_date_key(row.get("date") or datetime.now().date().isoformat())
            variant_key = self._variant_key_from_mapping(row)
            grouped_rows.setdefault((trade_date, *variant_key), []).append(row)

        return [
            self._build_daily_summary_row(trade_date, rows)
            for (trade_date, _), rows in sorted(
                grouped_rows.items(),
                key=lambda item: (item[0][0], str(item[0][1])),
            )
        ]

    def _render_run_summary_rows(self) -> list[dict[str, Any]]:
        daily_summary_rows = self._render_daily_summary_rows()
        if not daily_summary_rows:
            return []

        summary_frame = pd.DataFrame(daily_summary_rows, columns=DAILY_SUMMARY_COLUMNS)
        rendered_rows: list[dict[str, Any]] = []
        variant_groups = summary_frame.groupby(["variant_id"], dropna=False, sort=True)
        for _, variant_frame in variant_groups:
            for validity_bucket in ("valid_only", "all_days"):
                scoped_frame = variant_frame
                if validity_bucket == "valid_only":
                    scoped_frame = variant_frame[variant_frame["valid"] == True]
                if scoped_frame.empty:
                    continue
                rendered_rows.append(
                    self._build_run_summary_row(
                        scoped_frame.copy(),
                        validity_bucket=validity_bucket,
                    )
                )
        return rendered_rows

    def _build_daily_summary_row(self, trade_date: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        ordered_rows = sorted(rows, key=lambda row: str(row.get("ts") or ""))
        last_row = ordered_rows[-1]

        def last_non_null(column: str) -> Any:
            for row in reversed(ordered_rows):
                value = row.get(column)
                if value is not None:
                    return value
            return None

        entry_time = next(
            (
                row.get("ts")
                for row in ordered_rows
                if row.get("pos_evt") == "open" or (row.get("entry_px") is not None and int(row.get("qty") or 0) > 0)
            ),
            None,
        )
        exit_time = next(
            (
                row.get("ts")
                for row in reversed(ordered_rows)
                if row.get("pos_evt") == "close" or row.get("state") == "DONE"
            ),
            last_row.get("ts"),
        )

        entry_price = next((row.get("entry_px") for row in ordered_rows if row.get("entry_px") is not None), None)
        exit_price = next((row.get("exit_px") for row in reversed(ordered_rows) if row.get("exit_px") is not None), None)
        realized_pnl = next((row.get("r_pnl") for row in reversed(ordered_rows) if row.get("r_pnl") is not None), None)

        total_pnl_values = [float(row["t_pnl"]) for row in ordered_rows if row.get("t_pnl") is not None]
        quality_live_count = sum(int(row.get("q_live") or 0) for row in ordered_rows)
        quality_ffill_count = sum(int(row.get("q_ffill") or 0) for row in ordered_rows)
        quality_untrusted_count = sum(int(row.get("q_untrust") or 0) for row in ordered_rows)
        quality_missing_count = sum(int(row.get("q_miss") or 0) for row in ordered_rows)
        trusted_quotes = quality_live_count + quality_ffill_count
        total_quotes = trusted_quotes + quality_untrusted_count + quality_missing_count
        pct_trusted_quotes = (trusted_quotes / total_quotes * 100.0) if total_quotes else None
        valid_day_metrics = self._evaluate_valid_day(ordered_rows)

        summary = {
            # Day summary
            "date": trade_date,
            "variant_id": last_non_null("variant_id") or "default",
            "wd": self._weekday_name(trade_date),
            "ent_ts": entry_time,
            "ex_ts": exit_time,
            "ent_px": entry_price,
            "ex_px": exit_price,
            "r_pnl": realized_pnl,
            "min_t_pnl": min(total_pnl_values) if total_pnl_values else None,
            "max_t_pnl": max(total_pnl_values) if total_pnl_values else None,
            "exit_rsn": next((row.get("exit_rsn") for row in reversed(ordered_rows) if row.get("exit_rsn")), None),
            "state_end": last_row.get("state"),
            "ticks": len(ordered_rows),
            "fills": sum(int(row.get("fills") or 0) for row in ordered_rows),

            # Quote quality
            "pct_q_ok": round(pct_trusted_quotes, 2) if pct_trusted_quotes is not None else None,
            "q_live": quality_live_count,
            "q_ffill": quality_ffill_count,
            "q_untrust": quality_untrusted_count,
            "q_miss": quality_missing_count,
            "valid": valid_day_metrics["valid"],
            "pct_trusted_rows_window": round(valid_day_metrics["pct_trusted_rows_window"], 2),
            "max_untrusted_gap_seconds": round(valid_day_metrics["max_untrusted_gap_seconds"], 2),

            # Live runtime snapshot
            "ph_n": last_non_null("ph_n"),
            "mark_pts": last_non_null("mark_pts"),
            "be_lo": last_non_null("be_lo"),
            "be_hi": last_non_null("be_hi"),
            "sl_base": last_non_null("sl_base"),
            "sl_eff": last_non_null("sl_eff"),
            "tp_live": last_non_null("tp_live"),
            "tr_dist": last_non_null("tr_dist"),
            "tr_on": last_non_null("tr_on"),

            # Phase 1
            "sl_p1": last_non_null("sl_p1"),
            "tp_p1": last_non_null("tp_p1"),
            "act_p1": last_non_null("act_p1"),
            "tr_dist_p1": last_non_null("tr_dist_p1"),

            # Phase 2
            "sl_p2": last_non_null("sl_p2"),
            "tp_p2": last_non_null("tp_p2"),
            "tr_on_p2": last_non_null("tr_on_p2"),
            "act_p2": last_non_null("act_p2"),
            "tr_dist_p2": last_non_null("tr_dist_p2"),

            # Phase 3
            "sl_p3": last_non_null("sl_p3"),
            "tp_p3": last_non_null("tp_p3"),
            "tr_on_p3": last_non_null("tr_on_p3"),
            "act_p3": last_non_null("act_p3"),
            "tr_dist_p3": last_non_null("tr_dist_p3"),
        }
        return summary

    def _evaluate_valid_day(self, ordered_rows: list[dict[str, Any]]) -> dict[str, Any]:
        if not ordered_rows:
            return {
                "valid": False,
                "pct_trusted_rows_window": 0.0,
                "max_untrusted_gap_seconds": 0.0,
            }

        threshold_pct = float(self.valid_day_policy.get("pct_trusted_rows_in_window", 100.0) or 0.0)
        max_gap_seconds = float(self.valid_day_policy.get("max_gap_seconds_not_trusted", 0.0) or 0.0)

        relevant_rows = [row for row in ordered_rows if self._row_has_quality_signal(row)]
        if not relevant_rows:
            return {
                "valid": False,
                "pct_trusted_rows_window": 0.0,
                "max_untrusted_gap_seconds": 0.0,
            }

        trusted_flags = [self._is_trusted_row(row) for row in relevant_rows]
        trusted_ratio = sum(1 for flag in trusted_flags if flag) / len(trusted_flags) * 100.0
        observed_max_gap = self._max_untrusted_gap_seconds(relevant_rows, trusted_flags)
        return {
            "valid": trusted_ratio >= threshold_pct and observed_max_gap <= max_gap_seconds,
            "pct_trusted_rows_window": trusted_ratio,
            "max_untrusted_gap_seconds": observed_max_gap,
        }

    @staticmethod
    def _row_has_quality_signal(row: dict[str, Any]) -> bool:
        return any(
            int(row.get(column) or 0) > 0
            for column in ("q_live", "q_ffill", "q_untrust", "q_miss", "opt_q", "opt_q_all")
        )

    @staticmethod
    def _is_trusted_row(row: dict[str, Any]) -> bool:
        trusted_quotes = int(row.get("q_live") or 0) + int(row.get("q_ffill") or 0)
        untrusted_quotes = int(row.get("q_untrust") or 0) + int(row.get("q_miss") or 0)
        return trusted_quotes > 0 and untrusted_quotes == 0

    def _max_untrusted_gap_seconds(self, ordered_rows: list[dict[str, Any]], trusted_flags: list[bool]) -> float:
        cadence_seconds = self._infer_row_cadence_seconds(ordered_rows)
        max_gap = 0.0
        cluster_start: datetime | None = None
        cluster_end: datetime | None = None

        for row, is_trusted in zip(ordered_rows, trusted_flags, strict=False):
            timestamp = self._coerce_timestamp(row.get("ts"))
            if timestamp is None:
                continue
            if is_trusted:
                if cluster_start is not None and cluster_end is not None:
                    max_gap = max(max_gap, (cluster_end - cluster_start).total_seconds() + cadence_seconds)
                cluster_start = None
                cluster_end = None
                continue
            if cluster_start is None:
                cluster_start = timestamp
            cluster_end = timestamp

        if cluster_start is not None and cluster_end is not None:
            max_gap = max(max_gap, (cluster_end - cluster_start).total_seconds() + cadence_seconds)
        return max_gap

    @staticmethod
    def _coerce_timestamp(value: Any) -> datetime | None:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError:
                return None
        return None

    def _infer_row_cadence_seconds(self, ordered_rows: list[dict[str, Any]]) -> float:
        timestamps = [self._coerce_timestamp(row.get("ts")) for row in ordered_rows]
        timestamps = [ts for ts in timestamps if ts is not None]
        if len(timestamps) < 2:
            return 5.0
        deltas = []
        for left, right in zip(timestamps, timestamps[1:], strict=False):
            delta = (right - left).total_seconds()
            if delta > 0:
                deltas.append(delta)
        return min(deltas) if deltas else 5.0

    def _build_run_summary_row(
        self,
        summary_frame: pd.DataFrame,
        *,
        validity_bucket: str,
    ) -> dict[str, Any]:
        pnl = pd.to_numeric(summary_frame["r_pnl"], errors="coerce")
        intraday_drawdown = pd.to_numeric(summary_frame["min_t_pnl"], errors="coerce")
        pnl_non_null = pnl.dropna()
        win_days = int((pnl > 0).sum())
        loss_days = int((pnl < 0).sum())
        num_days = int(len(summary_frame))
        flat_days = num_days - win_days - loss_days

        wins = pnl[pnl > 0]
        losses = pnl[pnl < 0]
        exit_reasons = summary_frame["exit_rsn"].fillna("").astype(str)
        num_sl1_exit = int((exit_reasons == "sl1").sum())
        num_sl2_exit = int((exit_reasons == "sl2").sum())
        num_sl3_exit = int((exit_reasons == "sl3").sum())
        num_ts1_exit = int((exit_reasons == "ts1").sum())
        num_ts2_exit = int((exit_reasons == "ts2").sum())
        num_ts3_exit = int((exit_reasons == "ts3").sum())
        num_tp1_exit = int((exit_reasons == "tp1").sum())
        num_tp2_exit = int((exit_reasons == "tp2").sum())
        num_tp3_exit = int((exit_reasons == "tp3").sum())
        num_time_exit = int((exit_reasons == "time").sum())

        num_sl_exit = num_sl1_exit + num_sl2_exit + num_sl3_exit
        num_ts_exit = num_ts1_exit + num_ts2_exit + num_ts3_exit
        num_tp_exit = num_tp1_exit + num_tp2_exit + num_tp3_exit
        total_exits = num_sl_exit + num_ts_exit + num_tp_exit + num_time_exit

        gross_wins = float(wins.sum()) if not wins.empty else 0.0
        gross_losses = float((-losses).sum()) if not losses.empty else 0.0

        return {
            "run_id": self.run_id,
            "variant_id": self._series_first(summary_frame["variant_id"]) or "default",
            "validity_bucket": validity_bucket,
            "num_days": num_days,
            "win_days": win_days,
            "loss_days": loss_days,
            "flat_days": flat_days,
            "total_pnl": self._series_sum(pnl_non_null),
            "mean_pnl": self._series_mean(pnl_non_null),
            "median_pnl": self._series_median(pnl_non_null),
            "std_pnl": self._series_std(pnl_non_null),
            "min_pnl": self._series_min(pnl_non_null),
            "max_pnl": self._series_max(pnl_non_null),
            "p50_pnl": self._series_quantile(pnl_non_null, 0.50),
            "p95_pnl": self._series_quantile(pnl_non_null, 0.95),
            "avg_win_pnl": self._series_mean(wins),
            "avg_loss_pnl": self._series_mean(losses),
            "max_day_gain": self._series_max(pnl_non_null),
            "max_day_loss": self._series_min(pnl_non_null),
            "win_rate": self._safe_ratio(win_days, num_days),
            "profit_factor": self._safe_ratio(gross_wins, gross_losses),
            "payoff_ratio": self._safe_ratio(self._series_mean(wins), abs(self._series_mean(losses) or 0.0)),
            "num_sl1_exit": num_sl1_exit,
            "num_sl2_exit": num_sl2_exit,
            "num_sl3_exit": num_sl3_exit,
            "num_ts1_exit": num_ts1_exit,
            "num_ts2_exit": num_ts2_exit,
            "num_ts3_exit": num_ts3_exit,
            "num_tp1_exit": num_tp1_exit,
            "num_tp2_exit": num_tp2_exit,
            "num_tp3_exit": num_tp3_exit,
            "num_time_exit": num_time_exit,
            "pct_exit_sl": self._safe_ratio(num_sl_exit, total_exits),
            "pct_exit_ts": self._safe_ratio(num_ts_exit, total_exits),
            "pct_exit_tp": self._safe_ratio(num_tp_exit, total_exits),
            "pct_exit_time": self._safe_ratio(num_time_exit, total_exits),
            "avg_intraday_drawdown": self._series_mean(intraday_drawdown.dropna()),
            "max_intraday_drawdown": self._series_min(intraday_drawdown.dropna()),
            "worst_exit_pnl": self._series_min(pnl_non_null),
        }

    def _render_run_summary_section(self) -> list[str]:
        rows = self._render_run_summary_rows()
        if not rows:
            return ["  No run summary rows recorded."]

        summary_rows = [
            {
                "variant_id": str(row.get("variant_id") or "default"),
                "scope": row['validity_bucket'],
                "days": self._format_summary_metric_value(row.get("num_days")),
                "total_pnl": self._format_summary_metric_value(row.get("total_pnl")),
                "mean_pnl": self._format_summary_metric_value(row.get("mean_pnl")),
                "win_rate": self._format_summary_metric_value(row.get("win_rate")),
                "profit_factor": self._format_summary_metric_value(row.get("profit_factor")),
                "max_gain": self._format_summary_metric_value(row.get("max_day_gain")),
                "max_loss": self._format_summary_metric_value(row.get("max_day_loss")),
                "pct_sl": self._format_summary_metric_value(row.get("pct_exit_sl")),
                "pct_tp": self._format_summary_metric_value(row.get("pct_exit_tp")),
                "pct_time": self._format_summary_metric_value(row.get("pct_exit_time")),
            }
            for row in rows
        ]
        return self._render_text_table(
            columns=[
                "variant_id",
                "scope",
                "days",
                "total_pnl",
                "mean_pnl",
                "win_rate",
                "profit_factor",
                "max_gain",
                "max_loss",
                "pct_sl",
                "pct_tp",
                "pct_time",
            ],
            rows=summary_rows,
            right_align_columns={
                "days",
                "total_pnl",
                "mean_pnl",
                "win_rate",
                "profit_factor",
                "max_gain",
                "max_loss",
                "pct_sl",
                "pct_tp",
                "pct_time",
            },
        )

    def _render_text_table(
        self,
        *,
        columns: list[str],
        rows: list[dict[str, str]],
        totals: dict[str, str] | None = None,
        right_align_columns: set[str] | None = None,
        header_separator: bool = True,
        totals_separator_style: str = "column",
    ) -> list[str]:
        right_align = right_align_columns or set()
        widths = {
            column: max(
                len(column),
                max((len(str(row.get(column, ""))) for row in rows), default=0),
                len(str(totals.get(column, ""))) if totals else 0,
            )
            for column in columns
        }

        def render_row(row: dict[str, str]) -> str:
            rendered_cells = []
            for column in columns:
                value = str(row.get(column, ""))
                rendered_cells.append(value.rjust(widths[column]) if column in right_align else value.ljust(widths[column]))
            return "  " + " | ".join(rendered_cells)

        separator = "  " + "-+-".join("-" * widths[column] for column in columns)
        rendered_lines = [render_row({column: column for column in columns})]
        if header_separator:
            rendered_lines.append(separator)
        rendered_lines.extend(render_row(row) for row in rows)
        if totals:
            if totals_separator_style == "full":
                rendered_lines.append("  " + "-" * (len(rendered_lines[0]) - 2))
            else:
                rendered_lines.append(separator)
            rendered_lines.append(render_row(totals))
        return rendered_lines

    def _format_summary_metric_value(self, value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, float):
            return f"{value:.4f}".rstrip("0").rstrip(".")
        return str(value)

    def _series_sum(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.sum())

    def _series_first(self, series: pd.Series) -> Any:
        for value in series:
            if value is not None and not pd.isna(value):
                return value
        return None

    def _series_mean(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.mean())

    def _series_median(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.median())

    def _series_std(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.std(ddof=0))

    def _series_min(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.min())

    def _series_max(self, series: pd.Series) -> float | None:
        if series.empty:
            return None
        return float(series.max())

    def _series_quantile(self, series: pd.Series, quantile: float) -> float | None:
        if series.empty:
            return None
        return float(series.quantile(quantile))

    def _safe_ratio(self, numerator: float | int | None, denominator: float | int | None) -> float | None:
        if numerator is None or denominator in (None, 0):
            return None
        return float(numerator) / float(denominator)

    def _normalize_trade_date_key(self, trade_date: str | date) -> str:
        if isinstance(trade_date, date):
            return trade_date.isoformat()
        text = str(trade_date)
        if len(text) == 8 and text.isdigit():
            return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
        return text

    def _weekday_name(self, trade_date: str) -> str:
        return datetime.fromisoformat(self._normalize_trade_date_key(trade_date)).strftime("%A")

    def _action_trade_date(self, action: dict[str, Any]) -> str:
        fields = action.get("fields", {}) or {}
        if isinstance(fields, dict):
            trade_date = fields.get("trade_date")
            if isinstance(trade_date, str) and trade_date:
                return self._normalize_trade_date_key(trade_date)
        timestamp = action.get("timestamp")
        if isinstance(timestamp, datetime):
            return timestamp.date().isoformat()
        return datetime.now().date().isoformat()

    def _instrument_trade_date(self, stats: dict[str, Any]) -> str:
        trade_date = stats.get("trade_date")
        if isinstance(trade_date, str) and trade_date:
            return self._normalize_trade_date_key(trade_date)
        return datetime.now().date().isoformat()

    def _contextualize_fields(self, fields: dict[str, Any]) -> dict[str, Any]:
        contextualized = dict(fields)
        for key, value in self._current_variant_fields().items():
            contextualized.setdefault(key, value)
        return contextualized

    def _current_variant_fields(self) -> dict[str, str | float | None]:
        return dict(self._variant_context)

    def _variant_key_from_mapping(self, mapping: dict[str, Any]) -> tuple[str]:
        return (
            str(mapping.get("variant_id") or "default"),
        )

    def _variant_label(self, *, variant_id: str) -> str:
        return variant_id

    def _render_action_detail(self, fields: dict[str, Any]) -> str:
        if not fields:
            return "-"
        fragments = []
        for key, value in fields.items():
            fragments.append(f"{key}={self._format_field_value(value)}")
        return " ".join(fragments)