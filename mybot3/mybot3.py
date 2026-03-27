"""Entrypoint for the mybot3 pseudocode trading session."""

import argparse
import inspect
import socket
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from mybot3.ibkr_broker import IBKRBroker
from mybot3.processed_broker import ProcessedBroker, ProcessedReplayInput
from mybot3.risk_days import DEFAULT_RISK_EVENTS_PATH, RiskDayManager, RiskDayStatus
from mybot3.trading_run_output import TradingRunOutput
from mybot3.trading_session import TimedWalkthroughPolicy, TradingPhase, TradingSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed"
IBKR_PAPER_PORT = 7497
IBKR_LIVE_PORT = 7496
IBKR_DEFAULT_CLIENT_ID = 101


# Copied from config.yaml:
# - strategies.mybot_sltp.market_start_time / market_end_time
# - risk_policies.3phases_12_14-30_mybot
# - analysis.valid_day
MARKET_START_TIME_LOCAL = "10:00"
MARKET_END_TIME_LOCAL = "15:30"

VALID_DAY_POLICY = {
    # Replay-quality gate for daily/run summaries.
    # Example effect: a day is invalid when more than 120s of consecutive untrusted data
    # appears inside the evaluated window, or when trusted rows drop below 80%.
    "max_gap_seconds_not_trusted": 120,
    "pct_trusted_rows_in_window": 80.0,
}

RISK_DAY_POLICY = {
    # Trade-calendar gate for session entry.
    # False => if trade_date is tagged by risk_events.json (for example CPI/FOMC/NFP),
    # the session stays FLAT and no entry orders are submitted.
    # True  => risk days are allowed and only tagged in the outputs.
    "trade_on_risk_days": True,
}

ZONE_TIMES_LOCAL = ["12:00", "14:30"]

PHASE_1 = {
    "name": "phase_1",
    "window_start": MARKET_START_TIME_LOCAL,
    "window_end": ZONE_TIMES_LOCAL[0],
    "stop_loss": -600,
    "take_profit": 400,
    "activation_profit": 250,
    "trail_distance": 150,
}

PHASE_2 = {
    "name": "phase_2",
    "window_start": ZONE_TIMES_LOCAL[0],
    "window_end": ZONE_TIMES_LOCAL[1],
    "stop_loss": -600,
    "take_profit": 300,
    "activation_profit": 200,
    "trail_distance": 150,
}

PHASE_3 = {
    "name": "phase_3",
    "window_start": ZONE_TIMES_LOCAL[1],
    "window_end": MARKET_END_TIME_LOCAL,
    "stop_loss": -500,
    "take_profit": 300,
    "activation_profit": 150,
    "trail_distance": 75,
}

LOG_LEVEL_CHOICES = ["NOTSET", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
NEW_YORK_TIMEZONE = ZoneInfo("America/New_York")


def available_processed_sources() -> list[str]:
    return ProcessedBroker.available_sources(PROCESSED_ROOT)


def parse_iso_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def validate_processed_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    # Processed replay has two location styles:
    # 1) --processed-source <vendor> resolves files below data/processed/<vendor>/...
    # 2) --processed-market-dir <run_dir>/market replays a previously captured live/paper session.
    # Exactly one of those location inputs must be provided when --processed is active.
    processed_location_args = [args.processed_source, args.processed_market_dir]
    processed_args = [args.start_date, args.end_date]
    timed_args = [args.test_scenario, args.be_after_seconds, args.exit_after_seconds]
    if args.processed:
        if any(value is None for value in processed_args):
            parser.error("--processed requires --start-date and --end-date")
        if args.processed_source is None and args.processed_market_dir is None:
            parser.error("--processed requires either --processed-source or --processed-market-dir")
        if args.processed_source is not None and args.processed_market_dir is not None:
            parser.error("--processed-source and --processed-market-dir are mutually exclusive")
        if args.start_date > args.end_date:
            parser.error("--start-date must be less than or equal to --end-date")
        return

    if any(value is not None for value in processed_location_args + processed_args):
        parser.error("--processed-source, --processed-market-dir, --start-date, and --end-date are only allowed with --processed")
    # Timer-based test scenarios are shared by paper/live/processed, but the numeric
    # timer options only make sense when a concrete scenario is selected.
    if any(value is not None for value in timed_args[1:]) and args.test_scenario is None:
        parser.error("--be-after-seconds and --exit-after-seconds require --test-scenario")
    if args.test_scenario is not None and not (args.paper or args.live):
        parser.error("--test-scenario is only allowed with --paper, --live, or --processed")
    if args.test_scenario == "timed-walkthrough":
        # The walkthrough intentionally forces BE/exit transitions off timestamps,
        # so both delays must be explicitly configured and positive.
        if args.be_after_seconds is None or args.exit_after_seconds is None:
            parser.error("--test-scenario timed-walkthrough requires --be-after-seconds and --exit-after-seconds")
        if float(args.be_after_seconds) <= 0 or float(args.exit_after_seconds) <= 0:
            parser.error("--be-after-seconds and --exit-after-seconds must be greater than zero")


def build_risk_phases(*, sl1: float | None = None, sl2: float | None = None, sl3: float | None = None) -> list[TradingPhase]:
    phase_1 = {**PHASE_1, "stop_loss": PHASE_1["stop_loss"] if sl1 is None else sl1}
    phase_2 = {**PHASE_2, "stop_loss": PHASE_2["stop_loss"] if sl2 is None else sl2}
    phase_3 = {**PHASE_3, "stop_loss": PHASE_3["stop_loss"] if sl3 is None else sl3}
    return [
        TradingPhase(**phase_1),
        TradingPhase(**phase_2),
        TradingPhase(**phase_3),
    ]


def processed_source_label(args: argparse.Namespace) -> str:
    if getattr(args, "processed_market_dir", None):
        # Market-dir replays do not have a vendor source name, so derive a readable
        # label from the parent run directory for run_id/log naming.
        market_dir = Path(args.processed_market_dir)
        parent_name = market_dir.parent.name.strip()
        return f"market_{parent_name}" if parent_name else "market_capture"
    return str(args.processed_source)


def resolve_runtime(args: argparse.Namespace) -> tuple[str, object, Path]:
    if args.live:
        return "live", IBKRBroker(port=IBKR_LIVE_PORT, client_id=IBKR_DEFAULT_CLIENT_ID, max_session_updates=None), PROJECT_ROOT / "data" / "performance" / "live"
    if args.paper:
        return "paper", IBKRBroker(port=IBKR_PAPER_PORT, client_id=IBKR_DEFAULT_CLIENT_ID, max_session_updates=None), PROJECT_ROOT / "data" / "performance" / "paper"
    if args.processed_market_dir is not None:
        # Rehydrate a replay directly from a captured run_dir/market folder.
        # Output still goes under the regular replay area because this is a backtest-style run.
        return "processed", ProcessedBroker.from_market_dir(
            market_dir=args.processed_market_dir,
            start_date=args.start_date,
            end_date=args.end_date,
            source=processed_source_label(args),
        ), PROJECT_ROOT / "data" / "backtests" / "replay" / "mybot3"
    # Standard processed replay path from curated vendor data under data/processed.
    return "processed", ProcessedBroker.from_date_range(
        source=args.processed_source,
        processed_root=PROCESSED_ROOT,
        start_date=args.start_date,
        end_date=args.end_date,
    ), PROJECT_ROOT / "data" / "backtests" / "replay" / "mybot3"


def resolve_session_inputs(mode: str, broker: object) -> list[ProcessedReplayInput | date]:
    if mode == "processed":
        # Processed mode expands into one replay input per resolved trade date.
        return list(getattr(broker, "replay_inputs", []) or [])
    # Live/paper runs operate on the current trading day only.
    return [date.today()]


def _build_run_parser() -> argparse.ArgumentParser:
    # Default trading/backtest entrypoint.
    # Examples:
    # - python -m mybot3.mybot3 --processed --processed-source databento --start-date 2025-10-16 --end-date 2025-10-16
    # - python -m mybot3.mybot3 --processed --processed-source databento --test-scenario timed-walkthrough --be-after-seconds 15 --exit-after-seconds 30 --start-date 2025-10-16 --end-date 2025-10-16
    # - python -m mybot3.mybot3 --paper --test-scenario timed-walkthrough --be-after-seconds 60 --exit-after-seconds 120
    parser = argparse.ArgumentParser(description="Run the mybot3 pseudocode state machine entrypoint.")
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--live", action="store_true", help="Run mybot3 in live mode with the IBKR live port.")
    mode_group.add_argument("--paper", action="store_true", help="Run mybot3 in paper mode with the IBKR paper port.")
    mode_group.add_argument("--processed", action="store_true", help="Run mybot3 in processed replay mode.")
    parser.add_argument(
        "--print-config",
        action="store_true",
        help="Print the copied market window and phase constants.",
    )
    parser.add_argument(
        "--write-market",
        action="store_true",
        help="Write seen market data during the session into run_dir/market as processed parquet files.",
    )
    parser.add_argument(
        "--to-excel-csv",
        action="store_true",
        help="Export run parquet files as Excel-friendly CSV under data/exports/excel with _excel.csv suffix.",
    )
    parser.add_argument(
        "--log-level",
        choices=LOG_LEVEL_CHOICES,
        default="INFO",
        help="Minimum log level for console and run.log output.",
    )
    parser.add_argument(
        "--test-scenario",
        choices=["timed-walkthrough"],
        help="Optional accelerated paper/live/processed scenario that advances states on timers while still using broker order and fill flows.",
    )
    parser.add_argument(
        "--be-after-seconds",
        type=float,
        help="Seconds after entry fill before timed-walkthrough submits the break-even reduction order.",
    )
    parser.add_argument(
        "--exit-after-seconds",
        type=float,
        help="Seconds after break-even fill before timed-walkthrough submits the final exit order.",
    )
    parser.add_argument(
        "--processed-source",
        choices=available_processed_sources(),
        help="Processed data source under data/processed, for example databento.",
    )
    parser.add_argument(
        "--processed-market-dir",
        type=Path,
        # Intended for replaying a previous live/paper capture written by --write-market.
        # Example:
        # - python -m mybot3.mybot3 --processed --processed-market-dir data/performance/paper/<run_id>/market --start-date 2026-03-26 --end-date 2026-03-26
        # Concrete example:
        # - python -m mybot3.mybot3 --processed --processed-market-dir data/performance/paper/20260326_101500_m3/market --start-date 2026-03-26 --end-date 2026-03-26
        help="Replay directly from a live/paper run_dir/market folder instead of data/processed/<source>/...",
    )
    parser.add_argument(
        "--start-date",
        type=parse_iso_date,
        help="Processed replay start date in YYYY-MM-DD. If equal to --end-date, only one day is used.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_iso_date,
        help="Processed replay end date in YYYY-MM-DD. If equal to --start-date, only one day is used.",
    )
    return parser


def _build_refresh_risk_days_parser() -> argparse.ArgumentParser:
    # Maintenance command for refreshing data/metadata/riskevents/risk_events.json.
    # Example:
    # - python -m mybot3.mybot3 refresh-risk-days --fred-api-key <KEY> --start-year 2026 --end-year 2026
    parser = argparse.ArgumentParser(description="Refresh macro risk days into risk_events.json")
    parser.add_argument("--fred-api-key", required=True, help="FRED API key for CPI/NFP release dates")
    parser.add_argument("--start-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--end-year", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--out", type=Path, default=DEFAULT_RISK_EVENTS_PATH)
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    raw_argv = list(argv) if argv is not None else sys.argv[1:]
    command = raw_argv[0] if raw_argv else None
    if command == "refresh-risk-days":
        # Explicit subcommand path for risk-calendar maintenance.
        parser = _build_refresh_risk_days_parser()
        args = parser.parse_args(raw_argv[1:])
        args.command = "refresh-risk-days"
        return args

    if command == "run":
        # Optional explicit alias so both of these work:
        # - python -m mybot3.mybot3 --processed ...
        # - python -m mybot3.mybot3 run --processed ...
        raw_argv = raw_argv[1:]

    parser = _build_run_parser()
    args = parser.parse_args(raw_argv)
    args.command = "run"
    validate_processed_args(parser, args)
    if args.processed:
        # Fail early during argument parsing if the requested replay files do not exist.
        # This keeps the CLI error close to the input mistake instead of failing later in main().
        try:
            if args.processed_market_dir is not None:
                ProcessedBroker.resolve_replay_inputs_from_market_dir(
                    market_dir=args.processed_market_dir,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
            else:
                ProcessedBroker.resolve_replay_inputs(
                    processed_root=PROCESSED_ROOT,
                    source=args.processed_source,
                    start_date=args.start_date,
                    end_date=args.end_date,
                )
        except ValueError as exc:
            parser.error(str(exc))
    return args


def resolve_git_commit(project_root: Path) -> str | None:
    head_path = project_root / ".git" / "HEAD"
    if not head_path.exists():
        return None
    head_text = head_path.read_text(encoding="utf-8").strip()
    if not head_text:
        return None
    if not head_text.startswith("ref:"):
        return head_text
    ref_path = project_root / ".git" / head_text.split(" ", 1)[1]
    if ref_path.exists():
        return ref_path.read_text(encoding="utf-8").strip() or None
    return None


def build_run_id(args: argparse.Namespace) -> str | None:
    if not args.processed:
        return None
    # Only processed runs need synthetic ids here because live/paper outputs already
    # manage their own timestamped run directories inside TradingRunOutput.
    start_ymd = args.start_date.strftime("%Y%m%d")
    end_ymd = args.end_date.strftime("%Y%m%d")
    date_suffix = start_ymd if start_ymd == end_ymd else f"{start_ymd}-{end_ymd}"
    return f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{processed_source_label(args)}_d{date_suffix}"


def _normalize_market_timestamp_to_new_york(value: object, *, source_timezone: str | None) -> str | None:
    if value is None:
        return None
    try:
        moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except Exception:
        return None

    if moment.tzinfo is None:
        try:
            origin_tz = timezone.utc if source_timezone in {None, "UTC"} else ZoneInfo(str(source_timezone))
        except Exception:
            origin_tz = timezone.utc
        moment = moment.replace(tzinfo=origin_tz)

    return moment.astimezone(NEW_YORK_TIMEZONE).isoformat()


def log_nyse_status_if_closed(*, broker: object, output: TradingRunOutput) -> None:
    get_market_open_info = getattr(broker, "get_market_open_info", None)
    if not callable(get_market_open_info):
        return

    try:
        market_info = dict(get_market_open_info(datetime.now(timezone.utc)) or {})
    except Exception:
        return

    is_open = bool(
        market_info.get(
            "is_open_trading_hours",
            market_info.get("is_open_liquid_hours", False),
        )
    )
    if is_open:
        return

    source_timezone = str(market_info.get("timezone") or "UTC")
    as_of = _normalize_market_timestamp_to_new_york(
        market_info.get("as_of_utc"),
        source_timezone="UTC",
    ) or _normalize_market_timestamp_to_new_york(
        market_info.get("as_of_local"),
        source_timezone=source_timezone,
    )
    next_open = _normalize_market_timestamp_to_new_york(
        market_info.get("next_trading_open_local") or market_info.get("next_liquid_open_local"),
        source_timezone=source_timezone,
    )

    output.log(
        "info",
        "NYSE status",
        status="CLOSED",
        as_of=as_of,
        next_open=next_open,
        timezone="America/New_York",
        exchange=market_info.get("exchange"),
    )


def resolve_risk_day_status(*, risk_day_manager: RiskDayManager, trade_date: date) -> RiskDayStatus:
    try:
        return risk_day_manager.get_risk_status(trade_date)
    except Exception:
        return RiskDayStatus(date=trade_date, is_risk_day=False, events=())


def main() -> int:
    args = parse_args()
    if getattr(args, "command", "run") == "refresh-risk-days":
        # Refresh command exits early and does not initialize trading runtime objects.
        manager = RiskDayManager(store_path=args.out)
        events = manager.refresh_from_web(
            fred_api_key=args.fred_api_key,
            start_year=args.start_year,
            end_year=args.end_year,
        )
        print(f"Wrote {len(events)} events to {args.out}")
        return 0

    mode, broker, output_dir = resolve_runtime(args)
    # Shared calendar instance for both output tagging and entry policy decisions.
    risk_day_manager = RiskDayManager()
    timed_walkthrough_policy = None
    if args.test_scenario == "timed-walkthrough":
        # Timed walkthrough is a deterministic smoke-test mode:
        # - entry still depends on the normal strategy flow
        # - BE and final exit are triggered by elapsed seconds instead of PnL thresholds
        # - in processed mode those timers follow replay timestamps, not wall-clock time
        timed_walkthrough_policy = TimedWalkthroughPolicy(
            be_after_seconds=float(args.be_after_seconds),
            exit_after_seconds=float(args.exit_after_seconds),
        )
    output_kwargs = {
        "base_dir": output_dir,
        "run_id": build_run_id(args),
        "write_market": args.write_market,
        "log_level": args.log_level,
    }
    if "valid_day_policy" in inspect.signature(TradingRunOutput).parameters:
        output_kwargs["valid_day_policy"] = VALID_DAY_POLICY
    if "emit_live_tick_lines" in inspect.signature(TradingRunOutput).parameters:
        output_kwargs["emit_live_tick_lines"] = mode in {"live", "paper"}
    if "risk_day_manager" in inspect.signature(TradingRunOutput).parameters:
        output_kwargs["risk_day_manager"] = risk_day_manager
    output = TradingRunOutput(**output_kwargs)
    output.write_run_metadata(
        {
            "run_id": output.run_id,
            "created_at": date.today().isoformat(),
            "command": " ".join(sys.argv),
            "argv": list(sys.argv),
            "mode": mode,
            "cwd": os.getcwd(),
            "workspace_root": str(PROJECT_ROOT),
            "output_dir": str(output.run_dir),
            "log_level": args.log_level,
            "write_market": args.write_market,
            "valid_day_policy": VALID_DAY_POLICY,
            "risk_day_policy": RISK_DAY_POLICY,
            "test_scenario": args.test_scenario,
            "be_after_seconds": args.be_after_seconds,
            "exit_after_seconds": args.exit_after_seconds,
            "processed_source": args.processed_source,
            "processed_market_dir": str(args.processed_market_dir) if args.processed_market_dir is not None else None,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "python_executable": sys.executable,
            "hostname": socket.gethostname(),
            "git_commit": resolve_git_commit(PROJECT_ROOT),
        }
    )

    output.log("info", "mybot3 pseudocode entrypoint", run_id=output.run_id, mode=mode, output_dir=str(output.run_dir))
    output.log("info", "market window configured", market_start=MARKET_START_TIME_LOCAL, market_end=MARKET_END_TIME_LOCAL)
    output.log("info", "valid day policy configured", **VALID_DAY_POLICY)
    output.log("info", "risk day policy configured", **RISK_DAY_POLICY)
    if timed_walkthrough_policy is not None:
        output.log(
            "info",
            "test scenario configured",
            scenario=args.test_scenario,
            be_after_seconds=timed_walkthrough_policy.be_after_seconds,
            exit_after_seconds=timed_walkthrough_policy.exit_after_seconds,
        )
    if mode in {"live", "paper"}:
        log_nyse_status_if_closed(broker=broker, output=output)
    if args.write_market:
        output.log("info", "market capture enabled", market_dir=str(output.market_dir), format="processed_parquet")

    if mode == "processed":
        # Surface the resolved replay span once before sessions start so the run log
        # immediately shows how many days were found and which input style was used.
        processed_inputs = list(getattr(broker, "replay_inputs", []) or [])
        output.log(
            "info",
            "processed replay configured",
            processed_source=processed_source_label(args),
            processed_market_dir=str(args.processed_market_dir) if args.processed_market_dir is not None else None,
            start_date=args.start_date.isoformat(),
            end_date=args.end_date.isoformat(),
            single_day=args.start_date == args.end_date,
            resolved_days=len(processed_inputs),
        )

    for session_input in resolve_session_inputs(mode, broker):
        trade_date = session_input.trade_date if isinstance(session_input, ProcessedReplayInput) else session_input
        # Resolve the macro calendar once per trade date and reuse it across sweep variants.
        risk_day_status = resolve_risk_day_status(risk_day_manager=risk_day_manager, trade_date=trade_date)
        output.set_variant_context(
            variant_id="default",
            sl1=None,
            sl2=None,
            sl3=None,
        )
        output.log(
            "info",
            "risk day status resolved",
            trade_date=trade_date.isoformat(),
            is_risk_day=risk_day_status.is_risk_day,
            categories=sorted({event.category for event in risk_day_status.events}),
            trade_on_risk_days=RISK_DAY_POLICY["trade_on_risk_days"],
        )
        output.log("info", "starting trading session", trade_date=trade_date.isoformat(), mode=mode)
        if isinstance(session_input, ProcessedReplayInput):
            output.log(
                "info",
                "processed replay input",
                trade_date=session_input.trade_date.isoformat(),
                index_file=str(session_input.index_file),
                options_file=str(session_input.options_file),
            )

        trading_session = TradingSession(
            broker=broker,
            output=output,
            trade_date=trade_date,
            market_start_time=MARKET_START_TIME_LOCAL,
            market_end_time=MARKET_END_TIME_LOCAL,
            phases=build_risk_phases(),
            timed_walkthrough_policy=timed_walkthrough_policy,
            risk_day_status=risk_day_status,
            trade_on_risk_days=bool(RISK_DAY_POLICY["trade_on_risk_days"]),
        )

        output.log("info", "session initialized", initial_state=trading_session.state, trade_date=trade_date.isoformat())
        if args.print_config:
            output.log("info", "risk phases configured", phases=[phase.as_dict() for phase in trading_session.phases])

        for update in broker.iter_session_updates(trade_date=trade_date):
            output.write_market_data(update)
            trading_session.on_tick(
                update.market_snapshot,
                update.broker_snapshot,
                update.timestamp,
            )
        output.log(
            "info",
            "runtime note",
            note="broker session updates were consumed and reconciled",
            trade_date=trade_date.isoformat(),
        )

    output.flush()
    output.close()
    if args.to_excel_csv:
        export_dir = output.export_files_to_excel_csv(project_root=PROJECT_ROOT)
        print(
            f"Excel CSV export completed: run_dir={output.run_dir} export_dir={export_dir} separator=';' decimal=','"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())