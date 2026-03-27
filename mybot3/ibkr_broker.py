"""IBKR broker adapter for the mybot3 protocol."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import json
import math
import time
import warnings
from typing import Any
from zoneinfo import ZoneInfo


def _ensure_event_loop() -> None:
    try:
        asyncio.get_running_loop()
        return
    except RuntimeError:
        pass
    asyncio.set_event_loop(asyncio.new_event_loop())


_ensure_event_loop()

try:
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message=".*asyncio.get_event_loop_policy.*deprecated.*",
            category=DeprecationWarning,
        )
        from ib_insync import IB, ComboLeg, Contract as IBContract, Index, LimitOrder, MarketOrder, Option, StopOrder, Ticker  # type: ignore
        from ib_insync import Order as IBOrder  # type: ignore
except Exception as exc:  # noqa: BLE001
    raise ImportError("ib_insync is required for IBKRBroker") from exc

from mybot3.broker import (
    BrokerSnapshot,
    BracketOrderSet,
    BracketSpec,
    BrokerOrder,
    ComboContract,
    ComboOrderRuntime,
    IronFlySpec,
    MarketSnapshot,
    MyBot3BrokerProtocol,
    SessionUpdate,
    StructureLeg,
    StructureQuote,
    StructureMarketDataRequest,
    build_combo_contract_from_structure,
    build_combo_order,
    build_entry_bracket_orders,
    build_iron_fly_structure_request,
    coerce_number,
    normalize_expiry_token,
    preferred_option_price,
    register_submitted_runtime_order,
    resolve_order_purpose,
)


def normalize_commission(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if abs(number) > 1e100:
        return None
    return number


@dataclass(frozen=True)
class _IBKRIndexQuote:
    time: datetime
    symbol: str
    last: float
    bid: float | None
    ask: float | None


@dataclass(frozen=True)
class _IBKROptionQuote:
    time: datetime
    underlying: str
    right: str
    strike: float
    expiry: datetime
    bid: float
    ask: float
    quality: str


class _IBKRMarketDataClient:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        client_id: int,
        snapshot_timeout: float,
        options_instrument: str,
        index_symbol: str = "SPX",
        exchange: str = "CBOE",
        smart_exchange: str = "SMART",
        contract_multiplier: int = 100,
    ) -> None:
        self.host = host
        self.port = int(port)
        self.client_id = int(client_id)
        self.snapshot_timeout = float(snapshot_timeout)
        self.index_symbol = index_symbol
        self.exchange = exchange
        self.smart_exchange = smart_exchange
        self.contract_multiplier = int(contract_multiplier)
        self.options_instrument = str(options_instrument or "spxw")
        self.option_trading_class = self._normalize_option_trading_class(self.options_instrument)

        self.ib = IB()
        self._connected = False
        self._spx_stream_ticker: Ticker | None = None
        self._option_stream_tickers: dict[tuple[str, float, str], Ticker] = {}

    def _ensure_connection(self) -> None:
        if self._connected:
            return
        last_error: Exception | None = None
        base_client_id = int(self.client_id)
        for offset in range(5):
            candidate_client_id = base_client_id + offset
            self.client_id = candidate_client_id
            if offset > 0:
                self.ib = IB()
            try:
                self.ib.connect(self.host, self.port, clientId=self.client_id, timeout=5)
                self._connected = True
                return
            except Exception as exc:
                last_error = exc
                self._connected = False
                try:
                    self.ib.disconnect()
                except Exception:
                    pass
        if last_error is not None:
            raise last_error

    def _wait_for_ticker(self, ticker: Ticker) -> None:
        start = time.time()
        while True:
            remaining = max(0.05, self.snapshot_timeout - (time.time() - start))
            self.ib.waitOnUpdate(timeout=remaining)

            has_data = any(
                value is not None
                for value in (
                    ticker.time,
                    ticker.last,
                    ticker.marketPrice(),
                    ticker.bid,
                    ticker.ask,
                    getattr(ticker, "close", None),
                )
            )
            if has_data:
                return
            if time.time() - start >= self.snapshot_timeout:
                raise TimeoutError("IBKR market data client: no market data in time window")

    @staticmethod
    def _num_or_none(value: Any) -> float | None:
        try:
            number = float(value)
            return None if math.isnan(number) else number
        except Exception:
            return None

    def _index_contract(self) -> Index:
        return Index(symbol=self.index_symbol, exchange=self.exchange, currency="USD")

    @staticmethod
    def _normalize_option_trading_class(options_instrument: str | None) -> str | None:
        instrument = str(options_instrument or "").strip().upper()
        if not instrument:
            return None
        return instrument

    def _option_contract(self, expiry: datetime, strike: int, right: str) -> Option:
        contract_kwargs: dict[str, Any] = {
            "symbol": self.index_symbol,
            "lastTradeDateOrContractMonth": expiry.strftime("%Y%m%d"),
            "strike": float(strike),
            "right": right,
            "exchange": self.smart_exchange,
            "currency": "USD",
            "multiplier": str(self.contract_multiplier),
        }
        if self.option_trading_class:
            contract_kwargs["tradingClass"] = self.option_trading_class
        return Option(**contract_kwargs)

    def _qualify_one_contract(self, contract: Index | Option, *, kind: str):
        qualified_contracts = list(self.ib.qualifyContracts(contract))
        if len(qualified_contracts) != 1:
            raise RuntimeError(
                f"IBKR market data client: expected exactly 1 qualified {kind} contract, got {len(qualified_contracts)}"
            )
        return qualified_contracts[0]

    def _option_stream_key(self, expiry: datetime, strike: int, right: str) -> tuple[str, float, str]:
        return (expiry.strftime("%Y%m%d"), float(strike), str(right).upper())

    def _get_spx_stream_ticker(self) -> Ticker:
        self._ensure_connection()
        if self._spx_stream_ticker is None:
            qualified = self._qualify_one_contract(self._index_contract(), kind="index")
            self._spx_stream_ticker = self.ib.reqMktData(qualified, snapshot=False)
            self.ib.sleep(3)
        return self._spx_stream_ticker

    def _get_option_stream_ticker(self, expiry: datetime, strike: int, right: str) -> Ticker:
        self._ensure_connection()
        key = self._option_stream_key(expiry, strike, right)
        ticker = self._option_stream_tickers.get(key)
        if ticker is None:
            qualified = self._qualify_one_contract(self._option_contract(expiry, strike, right), kind="option")
            ticker = self.ib.reqMktData(qualified, snapshot=False)
            self.ib.sleep(3)
            self._option_stream_tickers[key] = ticker
        return ticker

    @staticmethod
    def _parse_hours(raw_hours: str) -> dict[str, list[tuple[datetime, datetime]]]:
        sessions: dict[str, list[tuple[datetime, datetime]]] = {}
        if not raw_hours:
            return sessions

        def _parse_dt(point: str, base_day: datetime) -> datetime:
            point = point.strip()
            if ":" in point:
                day_part, time_part = point.split(":", 1)
                day = datetime.strptime(day_part, "%Y%m%d")
                return day.replace(hour=int(time_part[:2]), minute=int(time_part[2:4]), second=0, microsecond=0)
            return base_day.replace(hour=int(point[:2]), minute=int(point[2:4]), second=0, microsecond=0)

        for token in raw_hours.split(";"):
            token = token.strip()
            if not token or ":" not in token:
                continue
            day_part, hours_part = token.split(":", 1)
            if hours_part.upper() == "CLOSED":
                sessions[day_part] = []
                continue

            base_day = datetime.strptime(day_part, "%Y%m%d")
            day_sessions: list[tuple[datetime, datetime]] = []
            for window in hours_part.split(","):
                window = window.strip()
                if not window or "-" not in window:
                    continue
                start_raw, end_raw = window.split("-", 1)
                start_dt = _parse_dt(start_raw, base_day)
                end_dt = _parse_dt(end_raw, base_day)
                if end_dt <= start_dt:
                    end_dt = end_dt + timedelta(days=1)
                day_sessions.append((start_dt, end_dt))
            sessions[day_part] = day_sessions
        return sessions

    @staticmethod
    def _is_open_in_sessions(sessions: dict[str, list[tuple[datetime, datetime]]], now_local: datetime) -> bool:
        for start_dt, end_dt in sessions.get(now_local.strftime("%Y%m%d"), []):
            if start_dt <= now_local <= end_dt:
                return True
        return False

    @staticmethod
    def _next_open_in_sessions(sessions: dict[str, list[tuple[datetime, datetime]]], now_local: datetime) -> datetime | None:
        windows: list[tuple[datetime, datetime]] = []
        for day_windows in sessions.values():
            windows.extend(day_windows)
        windows.sort(key=lambda item: item[0])
        for start_dt, _ in windows:
            if start_dt > now_local:
                return start_dt
        return None

    def get_market_open_info(self, current_dt: datetime | None = None) -> dict[str, Any]:
        self._ensure_connection()
        qualified = self._qualify_one_contract(self._index_contract(), kind="index")
        details = self.ib.reqContractDetails(qualified)
        if not details:
            raise RuntimeError(f"IBKR market data client: no contract details for {self.index_symbol}")

        detail = details[0]
        contract_details = detail.contractDetails if hasattr(detail, "contractDetails") else detail
        timezone_name = getattr(contract_details, "timeZoneId", "UTC") or "UTC"
        try:
            local_timezone = ZoneInfo(timezone_name)
        except Exception:
            timezone_name = "UTC"
            local_timezone = ZoneInfo("UTC")

        now_utc = current_dt if current_dt is not None else datetime.now(timezone.utc)
        if now_utc.tzinfo is None:
            now_utc = now_utc.replace(tzinfo=timezone.utc)
        now_local = now_utc.astimezone(local_timezone).replace(tzinfo=None)

        trading_sessions = self._parse_hours(str(getattr(contract_details, "tradingHours", "") or ""))
        liquid_sessions = self._parse_hours(str(getattr(contract_details, "liquidHours", "") or ""))
        next_trading_open = self._next_open_in_sessions(trading_sessions, now_local)
        next_liquid_open = self._next_open_in_sessions(liquid_sessions, now_local)

        return {
            "symbol": self.index_symbol,
            "exchange": self.exchange,
            "timezone": timezone_name,
            "as_of_utc": now_utc.isoformat(),
            "as_of_local": now_local.isoformat(),
            "is_open_trading_hours": self._is_open_in_sessions(trading_sessions, now_local),
            "is_open_liquid_hours": self._is_open_in_sessions(liquid_sessions, now_local),
            "next_trading_open_local": next_trading_open.isoformat() if next_trading_open is not None else None,
            "next_liquid_open_local": next_liquid_open.isoformat() if next_liquid_open is not None else None,
        }

    def get_spx_index(self, current_dt: datetime) -> _IBKRIndexQuote:
        ticker = self._get_spx_stream_ticker()
        self._wait_for_ticker(ticker)
        last = self._num_or_none(ticker.last)
        market_price = self._num_or_none(ticker.marketPrice())
        bid = self._num_or_none(ticker.bid)
        ask = self._num_or_none(ticker.ask)
        close = self._num_or_none(getattr(ticker, "close", None))

        if last is None:
            if market_price is not None:
                last = market_price
            elif close is not None:
                last = close
            elif bid is not None and ask is not None:
                last = (bid + ask) / 2.0

        if last is None and bid is None and ask is None and market_price is None and close is None:
            raise RuntimeError("IBKR market data client: market data not available")

        return _IBKRIndexQuote(
            time=current_dt,
            symbol=self.index_symbol,
            last=float(last if last is not None else 0.0),
            bid=bid,
            ask=ask,
        )

    def get_spx_option_quote(self, current_dt: datetime, expiry: datetime, strike: int, right: str) -> _IBKROptionQuote:
        ticker = self._get_option_stream_ticker(expiry, strike, right)
        self._wait_for_ticker(ticker)

        bid = self._num_or_none(ticker.bid)
        ask = self._num_or_none(ticker.ask)
        market_price = self._num_or_none(ticker.marketPrice())

        if bid is None and ask is None and market_price is not None:
            bid = market_price
            ask = market_price
        if bid is None:
            bid = 0.0
        if ask is None:
            ask = 0.0
        if bid == 0.0 and ask == 0.0 and market_price is None:
            raise RuntimeError("IBKR market data client: option market data not available")

        return _IBKROptionQuote(
            time=current_dt,
            underlying=self.index_symbol,
            right=right,
            strike=float(strike),
            expiry=expiry,
            bid=float(bid),
            ask=float(ask),
            quality="live",
        )


@dataclass
class IBKRBroker(MyBot3BrokerProtocol):
    """
    Live IBKR-oriented broker adapter for mybot3.

    Market data comes from a local ib_insync-backed client using streaming
    subscriptions sampled every sample_interval_seconds. Orders and broker state
    are pulled from the same IB connection so replay and live can share the same
    local position/order semantics through ComboOrderRuntime.
    """

    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 101
    account: str | None = None
    market_data_started: bool = False
    subscribed_structures: list[str] | None = None
    requested_structure: StructureMarketDataRequest | None = None
    snapshot_timeout: float = 5.0
    options_instrument: str = "spxw"
    sample_interval_seconds: float = 5.0
    max_session_updates: int | None = 1
    _feed: object | None = None
    _qualified_leg_contracts: dict[tuple[str, float, str], Any] = field(default_factory=dict)
    _qualified_leg_contract_details: dict[tuple[str, float, str], Any] = field(default_factory=dict)
    _ib_trades: dict[str, Any] = field(default_factory=dict)
    _seen_fill_exec_ids: set[str] = field(default_factory=set)
    _synthetic_order_id: int = 1
    runtime: ComboOrderRuntime = field(init=False)

    def __post_init__(self) -> None:
        if self.subscribed_structures is None:
            self.subscribed_structures = []
        self.runtime = ComboOrderRuntime(source="ibkr")

    def build_iron_fly_combo_contract(self, spec: IronFlySpec) -> ComboContract:
        structure_request = build_iron_fly_structure_request(spec)
        return build_combo_contract_from_structure(
            broker="ibkr",
            structure_request=structure_request,
            quantity=spec.quantity,
            extra_fields={
                "secType": "BAG",
                "routing": "SMART",
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "account": self.account,
                "structure": {
                    "underlying": spec.underlying,
                    "expiry": spec.expiry,
                    "quantity": spec.quantity,
                    "short_call_strike": spec.short_call_strike,
                    "short_put_strike": spec.short_put_strike,
                    "long_call_strike": spec.long_call_strike,
                    "long_put_strike": spec.long_put_strike,
                },
            },
        )

    def build_combo_contract_for_structure(
        self,
        *,
        structure: StructureMarketDataRequest,
        quantity: int,
    ) -> ComboContract:
        return build_combo_contract_from_structure(
            broker="ibkr",
            structure_request=structure,
            quantity=quantity,
            extra_fields={
                "secType": "BAG",
                "routing": "SMART",
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "account": self.account,
            },
        )

    def build_entry_bracket_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        bracket: BracketSpec,
    ) -> BracketOrderSet:
        return build_entry_bracket_orders(
            broker="ibkr",
            combo_contract=combo_contract,
            bracket=bracket,
        )

    def build_exit_order_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        purpose: str,
        limit_price: float | None,
    ) -> BrokerOrder:
        return build_combo_order(
            broker="ibkr",
            combo_contract=combo_contract,
            purpose=purpose,
            order_type="LMT" if limit_price is not None else "MKT",
            limit_price=limit_price,
        )

    def submit_bracket(
        self,
        *,
        combo_contract: ComboContract,
        bracket_orders: BracketOrderSet,
    ) -> list[str]:
        # IBKR BAG parent/child brackets proved unreliable for the SPX iron-fly paper flow
        # because attached combo children can be rejected before the parent is recognized.
        # The session state machine already manages TP/SL/BE exits itself, so submit only
        # the entry order here and let later exits use explicit follow-up orders.
        parent_id = self._place_combo_order(
            combo_contract=combo_contract,
            order=bracket_orders.parent,
            purpose="entry",
            parent_order_id=None,
            transmit=True,
        )
        return [parent_id]

    def submit_order(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
    ) -> str:
        purpose = resolve_order_purpose(order)
        return self._place_combo_order(
            combo_contract=combo_contract,
            order=order,
            purpose=purpose,
            parent_order_id=None,
            transmit=True,
        )

    def ensure_structure_market_data(self, structure: StructureMarketDataRequest) -> None:
        self.requested_structure = structure
        structure_key = str(structure)
        if structure_key not in self.subscribed_structures:
            self.subscribed_structures.append(structure_key)

    def iter_session_updates(self, *, trade_date: date):
        self.market_data_started = True
        update_count = 0
        while True:
            event_time = datetime.now(timezone.utc)
            try:
                market_snapshot = self._build_live_market_snapshot(trade_date=trade_date, event_time=event_time)
            except Exception as exc:
                market_snapshot = self._build_fallback_market_snapshot(trade_date=trade_date, event_time=event_time, error=str(exc))

            broker_snapshot = self._build_broker_snapshot(trade_date=trade_date, event_time=event_time)
            yield SessionUpdate(
                timestamp=event_time,
                market_snapshot=market_snapshot,
                broker_snapshot=broker_snapshot,
            )

            update_count += 1
            if self.max_session_updates is not None and update_count >= self.max_session_updates:
                return
            self._ib_sleep(max(float(self.sample_interval_seconds), 0.0))

    def _build_live_market_snapshot(self, *, trade_date: date, event_time: datetime) -> MarketSnapshot:
        feed = self._get_feed()
        index_quote = feed.get_spx_index(event_time)
        option_quotes = self._load_option_quotes(feed, event_time)
        structure_quotes = {
            quote["label"]: preferred_option_price(quote)
            for quote in option_quotes
            if isinstance(quote.get("label"), str)
        }
        structure_quote_details = {
            quote["label"]: StructureQuote(
                label=str(quote["label"]),
                bid=coerce_number(quote.get("bid")),
                ask=coerce_number(quote.get("ask")),
                mid=coerce_number(quote.get("mid")),
            )
            for quote in option_quotes
            if isinstance(quote.get("label"), str)
        }
        ymd = trade_date.strftime("%Y%m%d")
        return MarketSnapshot(
            timestamp=event_time,
            underlying_price=float(index_quote.last),
            structure_quotes=structure_quotes,
            source="ibkr",
            metadata={
                "trade_date": trade_date.isoformat(),
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "market_data_mode": "streaming_subscription",
                "subscription_started": self.market_data_started,
                "subscribed_structures": list(self.subscribed_structures),
                "index_file": f"spx_ibkr_{ymd}.processed.parquet",
                "options_file": f"spxw_ibkr_{ymd}.processed.parquet",
                "index_row": self._index_row(index_quote),
                "option_quote_count": len(option_quotes),
                "option_quotes": option_quotes,
                "requested_structure": self.requested_structure,
                "feed_mode": "live_ibkr",
            },
            structure_quote_details=structure_quote_details,
        )

    def _build_fallback_market_snapshot(self, *, trade_date: date, event_time: datetime, error: str) -> MarketSnapshot:
        ymd = trade_date.strftime("%Y%m%d")
        option_quotes = self._missing_option_quotes()
        structure_quotes = {
            quote["label"]: None
            for quote in option_quotes
            if isinstance(quote.get("label"), str)
        }
        structure_quote_details = {
            quote["label"]: StructureQuote(
                label=str(quote["label"]),
                bid=coerce_number(quote.get("bid")),
                ask=coerce_number(quote.get("ask")),
                mid=coerce_number(quote.get("mid")),
            )
            for quote in option_quotes
            if isinstance(quote.get("label"), str)
        }
        return MarketSnapshot(
            timestamp=event_time,
            underlying_price=None,
            structure_quotes=structure_quotes,
            source="ibkr",
            metadata={
                "trade_date": trade_date.isoformat(),
                "host": self.host,
                "port": self.port,
                "client_id": self.client_id,
                "market_data_mode": "streaming_subscription",
                "subscription_started": self.market_data_started,
                "subscribed_structures": list(self.subscribed_structures),
                "index_file": f"spx_ibkr_{ymd}.processed.parquet",
                "options_file": f"spxw_ibkr_{ymd}.processed.parquet",
                "index_row": {
                    "symbol": "SPX",
                    "last": None,
                    "bid": None,
                    "ask": None,
                    "source": "ibkr",
                },
                "option_quote_count": len(option_quotes),
                "option_quotes": option_quotes,
                "requested_structure": self.requested_structure,
                "feed_mode": "fallback",
                "capture_error": error,
            },
            structure_quote_details=structure_quote_details,
        )

    def _build_broker_snapshot(self, *, trade_date: date, event_time: datetime) -> BrokerSnapshot:
        try:
            self._refresh_tracked_order_statuses()
            fills = self._collect_new_ib_fills(event_time=event_time)
        except Exception:
            fills = []
        return BrokerSnapshot(
            timestamp=event_time,
            open_orders=self.runtime.open_order_statuses(),
            fills=fills,
            positions=self.runtime.positions(),
            metadata={
                "broker": "ibkr",
                "account": self.account,
                "trade_date": trade_date.isoformat(),
                "source": "live_ibkr",
            },
        )

    def _place_combo_order(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
        purpose: str,
        parent_order_id: str | None,
        transmit: bool,
    ) -> str:
        ib = self._get_ib()
        ib_contract = self._ib_combo_contract(combo_contract=combo_contract, purpose=purpose)
        ib_order, order_id = self._ib_order_for_combo(
            combo_contract=combo_contract,
            order=order,
            purpose=purpose,
            parent_order_id=parent_order_id,
            transmit=transmit,
        )
        trade = ib.placeOrder(ib_contract, ib_order)
        retry_override = self._await_advanced_reject_override(ib=ib, trade=trade, timeout_seconds=2.0)
        tracked_trade = trade
        tracked_order_id = order_id
        if retry_override is not None and self._trade_has_terminal_reject(trade):
            retry_order, retry_order_id = self._ib_order_for_combo(
                combo_contract=combo_contract,
                order=order,
                purpose=purpose,
                parent_order_id=parent_order_id,
                transmit=transmit,
            )
            if isinstance(retry_order, IBOrder):
                retry_order.advancedErrorOverride = retry_override
            tracked_trade = ib.placeOrder(ib_contract, retry_order)
            tracked_order_id = retry_order_id

        local_order_id = str(tracked_order_id)
        self._ib_trades[local_order_id] = tracked_trade
        return register_submitted_runtime_order(
            self.runtime,
            order_id=local_order_id,
            combo_contract=combo_contract,
            order=order,
            purpose=purpose,
            parent_order_id=parent_order_id,
        )

    def _trade_has_terminal_reject(self, trade: Any) -> bool:
        order_status = getattr(trade, "orderStatus", None)
        status = str(getattr(order_status, "status", "") or "").strip().lower()
        return status in {"cancelled", "canceled", "api cancelled", "inactive", "rejected"}

    def _trade_runtime_status(self, trade: Any) -> str:
        order_status = getattr(trade, "orderStatus", None)
        raw_status = str(getattr(order_status, "status", "") or "")
        normalized_status = raw_status.strip().lower()
        if normalized_status in {"cancelled", "canceled", "api cancelled", "inactive", "rejected"}:
            advanced_error = getattr(trade, "advancedError", None)
            if isinstance(advanced_error, str) and advanced_error.strip():
                return "rejected"

            for entry in list(getattr(trade, "log", []) or []):
                error_code = int(getattr(entry, "errorCode", 0) or 0)
                message = str(getattr(entry, "message", "") or "").strip().lower()
                if error_code > 0 or "rejected" in message:
                    return "rejected"

        return raw_status

    def _extract_advanced_reject_override(self, trade: Any) -> str | None:
        payloads: list[str] = []
        advanced_error = getattr(trade, "advancedError", None)
        if isinstance(advanced_error, str) and advanced_error.strip():
            payloads.append(advanced_error.strip())

        for entry in list(getattr(trade, "log", []) or []):
            message = getattr(entry, "message", None)
            if isinstance(message, str) and message.strip().startswith("{"):
                payloads.append(message.strip())

        fixstrs: list[str] = []
        for payload in payloads:
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            rejects = parsed.get("rejects")
            if not isinstance(rejects, list):
                continue
            for reject in rejects:
                if not isinstance(reject, dict):
                    continue
                for button in reject.get("buttons", []):
                    if not isinstance(button, dict):
                        continue
                    for option in button.get("options", []):
                        if not isinstance(option, dict):
                            continue
                        fixstr = str(option.get("fixstr") or "").strip()
                        if fixstr and fixstr not in fixstrs:
                            fixstrs.append(fixstr)
        if not fixstrs:
            return None
        return ",".join(fixstrs)

    def _await_advanced_reject_override(self, *, ib: Any, trade: Any, timeout_seconds: float) -> str | None:
        override = self._extract_advanced_reject_override(trade)
        if override is not None:
            return override

        wait_on_update = getattr(ib, "waitOnUpdate", None)
        deadline = time.time() + max(0.0, float(timeout_seconds))
        while time.time() < deadline:
            remaining = max(0.0, deadline - time.time())
            delay = min(0.25, remaining)
            if callable(wait_on_update):
                try:
                    wait_on_update(timeout=delay)
                except TypeError:
                    wait_on_update(delay)
            elif delay > 0.0:
                self._ib_sleep(delay)

            override = self._extract_advanced_reject_override(trade)
            if override is not None:
                return override
            if self._trade_has_terminal_reject(trade):
                break
        return None

    def _refresh_tracked_order_statuses(self) -> None:
        for order_id, trade in list(self._ib_trades.items()):
            order_status = getattr(trade, "orderStatus", None)
            if order_status is None:
                continue
            self.runtime.update_order_status(
                order_id=order_id,
                status=self._trade_runtime_status(trade),
                filled_quantity=float(getattr(order_status, "filled", 0.0) or 0.0),
                remaining_quantity=float(getattr(order_status, "remaining", 0.0) or 0.0),
                avg_fill_price=float(getattr(order_status, "avgFillPrice", 0.0) or 0.0),
            )

    def _collect_new_ib_fills(self, *, event_time: datetime) -> list[Any]:
        ib = self._get_ib()
        broker_fills: list[Any] = []
        for raw_fill in list(ib.fills() or []):
            execution = getattr(raw_fill, "execution", None)
            exec_id = str(getattr(execution, "execId", "") or "")
            order_id = str(getattr(execution, "orderId", "") or "")
            if exec_id and exec_id in self._seen_fill_exec_ids:
                continue
            if not order_id:
                continue
            fill_price = coerce_number(getattr(execution, "price", None))
            if fill_price is None:
                continue
            commission_report = getattr(raw_fill, "commissionReport", None)
            commission = normalize_commission(getattr(commission_report, "commission", None))
            fill = self.runtime.apply_fill(
                order_id=order_id,
                fill_price=fill_price,
                timestamp=event_time,
                metadata={
                    "exec_id": exec_id or None,
                    "exec_time": getattr(execution, "time", None).isoformat() if getattr(execution, "time", None) is not None else None,
                    "side": str(getattr(execution, "side", "") or "") or None,
                    "commission": commission,
                    "broker_order_id": order_id,
                },
            )
            if fill is not None:
                broker_fills.append(fill)
            if exec_id:
                self._seen_fill_exec_ids.add(exec_id)
        return broker_fills

    def _get_feed(self):
        if self._feed is None:
            self._feed = _IBKRMarketDataClient(
                host=self.host,
                port=self.port,
                client_id=self.client_id,
                snapshot_timeout=self.snapshot_timeout,
                options_instrument=self.options_instrument,
            )
        return self._feed

    def _get_ib(self):
        feed = self._get_feed()
        ensure_connection = getattr(feed, "_ensure_connection", None)
        if callable(ensure_connection):
            ensure_connection()
        ib = getattr(feed, "ib", None)
        if ib is None:
            raise RuntimeError("IBKR market data client does not expose an IB connection")
        return ib

    def _ib_sleep(self, seconds: float) -> None:
        delay = max(float(seconds), 0.0)
        if delay <= 0.0:
            return
        try:
            ib = self._get_ib()
        except Exception:
            time.sleep(delay)
            return
        sleep = getattr(ib, "sleep", None)
        if callable(sleep):
            sleep(delay)
            return
        time.sleep(delay)

    def get_market_open_info(self, current_dt: datetime | None = None) -> dict[str, Any]:
        feed = self._get_feed()
        get_market_open_info = getattr(feed, "get_market_open_info", None)
        if not callable(get_market_open_info):
            raise RuntimeError("IBKR market data feed does not expose market-open info")
        return dict(get_market_open_info(current_dt) or {})

    def _load_option_quotes(self, feed, event_time: datetime) -> list[dict[str, object]]:
        if self.requested_structure is None:
            return []

        option_quotes: list[dict[str, object]] = []
        for leg in self.requested_structure.legs:
            expiry = datetime.fromisoformat(normalize_expiry_token(leg.expiry)).replace(tzinfo=timezone.utc)
            quote = feed.get_spx_option_quote(event_time, expiry, int(leg.strike), leg.right)
            option_quotes.append(self._option_row(leg.label, quote))
        return option_quotes

    def _missing_option_quotes(self) -> list[dict[str, object]]:
        if self.requested_structure is None:
            return []

        return [
            {
                "label": leg.label,
                "underlying": self.requested_structure.underlying,
                "right": leg.right,
                "strike": leg.strike,
                "expiry": leg.expiry,
                "bid": None,
                "ask": None,
                "mid": None,
                "quality": "missing",
            }
            for leg in self.requested_structure.legs
        ]

    @staticmethod
    def _index_row(index_quote: Any) -> dict[str, object]:
        return {
            "symbol": index_quote.symbol,
            "last": float(index_quote.last),
            "bid": float(index_quote.bid) if index_quote.bid is not None else None,
            "ask": float(index_quote.ask) if index_quote.ask is not None else None,
            "source": "ibkr",
        }

    @staticmethod
    def _option_row(label: str, quote: Any) -> dict[str, object]:
        mid = (float(quote.bid) + float(quote.ask)) / 2.0
        return {
            "label": label,
            "underlying": quote.underlying,
            "right": quote.right,
            "strike": float(quote.strike),
            "expiry": quote.expiry.date().isoformat(),
            "bid": float(quote.bid),
            "ask": float(quote.ask),
            "mid": mid,
            "quality": quote.quality,
        }

    def _qualified_option_contract(self, leg: StructureLeg):
        key = (leg.right.upper(), float(leg.strike), normalize_expiry_token(leg.expiry).replace("-", ""))
        if key in self._qualified_leg_contracts:
            return self._qualified_leg_contracts[key]

        feed = self._get_feed()
        expiry = datetime.fromisoformat(normalize_expiry_token(leg.expiry))
        contract = feed._option_contract(expiry, int(leg.strike), leg.right)
        qualified = feed._qualify_one_contract(contract, kind="option")
        self._qualified_leg_contracts[key] = qualified
        return qualified

    def _qualified_option_contract_details(self, leg: StructureLeg):
        key = (leg.right.upper(), float(leg.strike), normalize_expiry_token(leg.expiry).replace("-", ""))
        if key in self._qualified_leg_contract_details:
            return self._qualified_leg_contract_details[key]

        ib = self._get_ib()
        qualified = self._qualified_option_contract(leg)
        details = list(ib.reqContractDetails(qualified) or [])
        if not details:
            raise RuntimeError(f"IBKR combo contract: no contract details for {leg.label}")
        contract_details = details[0]
        self._qualified_leg_contract_details[key] = contract_details
        return contract_details

    @staticmethod
    def _valid_exchanges_from_contract_details(contract_details: Any) -> list[str]:
        detail = contract_details.contractDetails if hasattr(contract_details, "contractDetails") else contract_details
        raw_exchanges = str(getattr(detail, "validExchanges", "") or "")
        return [exchange.strip().upper() for exchange in raw_exchanges.split(",") if exchange.strip()]

    def _combo_exchange_for_structure(self, structure_request: StructureMarketDataRequest) -> str:
        # IBKR documents that combos with more than two legs must remain guaranteed,
        # which in practice means routing them directly to a common exchange instead
        # of SMART/non-guaranteed smart-combo routing.
        if len(structure_request.legs) <= 2:
            return "SMART"

        common_exchanges: list[str] | None = None
        for leg in structure_request.legs:
            leg_exchanges = self._valid_exchanges_from_contract_details(self._qualified_option_contract_details(leg))
            if not leg_exchanges:
                return "SMART"
            if common_exchanges is None:
                common_exchanges = leg_exchanges
                continue
            common_exchanges = [exchange for exchange in common_exchanges if exchange in set(leg_exchanges)]
            if not common_exchanges:
                return "SMART"

        direct_exchanges = [
            exchange
            for exchange in (common_exchanges or [])
            if exchange not in {"SMART", "VALUE"}
        ]
        if not direct_exchanges:
            return "SMART"

        preferred_exchanges = ("CBOE", "CBOE2", "BOX", "ISE", "PHLX", "AMEX", "MIAX")
        for exchange in preferred_exchanges:
            if exchange in direct_exchanges:
                return exchange
        return direct_exchanges[0]

    def _ib_combo_contract(self, *, combo_contract: ComboContract, purpose: str):
        structure_request = combo_contract.raw.get("structure_request")
        if not isinstance(structure_request, StructureMarketDataRequest):
            raise ValueError("Combo contract is missing structure_request")

        bag = IBContract()
        bag.symbol = structure_request.underlying
        bag.secType = "BAG"
        bag.currency = "USD"
        bag.exchange = self._combo_exchange_for_structure(structure_request)
        bag.comboLegs = []
        for leg in structure_request.legs:
            qualified = self._qualified_option_contract(leg)
            combo_leg = ComboLeg()
            combo_leg.conId = int(getattr(qualified, "conId"))
            combo_leg.ratio = 1
            combo_leg.action = self._combo_leg_action(leg=leg, purpose=purpose)
            combo_leg.exchange = bag.exchange
            bag.comboLegs.append(combo_leg)
        return bag

    def _combo_leg_action(self, *, leg: StructureLeg, purpose: str) -> str:
        is_short_leg = leg.label.startswith("short_")
        if purpose == "entry":
            return "SELL" if is_short_leg else "BUY"
        return "BUY" if is_short_leg else "SELL"

    def _ib_order_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
        purpose: str,
        parent_order_id: str | None,
        transmit: bool,
    ) -> tuple[Any, int]:
        quantity = int(combo_contract.raw.get("structure", {}).get("quantity", 1))
        order_type = str(order.raw.get("orderType") or "MKT").upper()
        order_action = "SELL" if purpose == "entry" else "BUY"
        limit_price = order.raw.get("lmtPrice")
        signed_limit_price = None
        if limit_price is not None:
            signed_limit_price = float(limit_price)
            if purpose == "entry":
                signed_limit_price = -abs(signed_limit_price)
        if order_type == "LMT":
            if signed_limit_price is None:
                raise ValueError("IBKR combo limit order requires lmtPrice")
            ib_order = LimitOrder(order_action, quantity, signed_limit_price)
        elif order_type == "STP":
            ib_order = StopOrder(order_action, quantity, float(order.raw.get("auxPrice")))
        else:
            ib_order = MarketOrder(order_action, quantity)

        order_id = self._next_order_id()
        if isinstance(ib_order, IBOrder):
            ib_order.orderId = order_id
            ib_order.transmit = bool(transmit)
            ib_order.orderRef = f"mybot3:{purpose}"
            if parent_order_id is not None:
                ib_order.parentId = int(parent_order_id)
            if self.account:
                ib_order.account = self.account
            ib_order.overridePercentageConstraints = True
            ib_order.advancedErrorOverride = "8229=COMBOPAYOUT"
        return ib_order, order_id

    def _next_order_id(self) -> int:
        ib = self._get_ib()
        client = getattr(ib, "client", None)
        get_req_id = getattr(client, "getReqId", None)
        if callable(get_req_id):
            return int(get_req_id())
        order_id = self._synthetic_order_id
        self._synthetic_order_id += 1
        return order_id
