"""
Trading session state machine for the mybot3 model.

Purpose:
- Keep the entrypoint thin.
- Group the state machine and its supporting phase/session objects in one module.
- Allow mybot3.py to instantiate a TradingSession with explicit phase parameters.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

from mybot3.broker import BracketSpec, BrokerFill, BrokerOrderStatus, BrokerPosition, IronFlySpec, MyBot3BrokerProtocol, StructureLeg, StructureMarketDataRequest, StructureQuote
from mybot3.trading_run_output import DailyDetailRow, TradingRunOutput


EVENT_TRANSITIONS: dict[tuple[str, str], str] = {
    ("FLAT", "entry_signal"): "ENTRY_PENDING",
    ("ENTRY_PENDING", "entry_rejected"): "FLAT",
    ("ENTRY_PENDING", "entry_fill_confirmed"): "IRON_FLY_ACTIVE",
    ("IRON_FLY_ACTIVE", "exit_triggered"): "EXIT_PENDING",
    ("IRON_FLY_ACTIVE", "break_even_triggered"): "BE_EXIT_PENDING",
    ("BE_EXIT_PENDING", "break_even_fill_confirmed"): "CREDIT_SPREAD_ACTIVE",
    ("CREDIT_SPREAD_ACTIVE", "exit_triggered"): "EXIT_PENDING",
    ("EXIT_PENDING", "exit_fill_confirmed"): "DONE",
}


@dataclass(frozen=True)
class SessionStructure:
    request: StructureMarketDataRequest
    quote_labels: tuple[str, ...]


@dataclass(frozen=True)
class IronFlyPosition:
    entry_mark: float
    entry_credit: float
    quantity: int
    center_strike: float
    wingsize: float


@dataclass(frozen=True)
class CreditSpreadPosition:
    side: str
    entry_mark: float
    entry_credit: float
    quantity: int


@dataclass(frozen=True)
class TimedWalkthroughPolicy:
    be_after_seconds: float
    exit_after_seconds: float


class TradingSession:
    """
    Main state machine logic.

    Main idea:
    - Start with one full Iron Fly.
    - Evaluate TP/SL on the whole Iron Fly.
    - If break-even triggers, close two legs.
    - After broker-confirmed fills, treat the remaining two legs as one Credit Spread.
    - Evaluate TP/SL on the whole Credit Spread.
    """

    def __init__(
        self,
        *,
        broker: MyBot3BrokerProtocol,
        output: TradingRunOutput,
        trade_date: date,
        market_start_time: str,
        market_end_time: str,
        take_profit_pct: float,
        stop_loss_pct: float,
        stop_loss_max: float,
        wingsize: float = 15,
        min_entry_credit: float = 5.0,
        quantity: int = 1,
        contract_multiplier: int = 100,
        entry_gate_disabled: bool = False,
        entry_gate_start_time: str | None = None,
        entry_gate_min_credit: float | None = None,
        entry_gate_time_tolerance_seconds: float = 0.0,
        timed_walkthrough_policy: TimedWalkthroughPolicy | None = None,
        enable_break_even: bool = True,
    ):
        self.state = "FLAT"
        self.broker = broker
        self.output = output
        self.trade_date = trade_date
        self.market_start_time = market_start_time
        self.market_end_time = market_end_time
        self.take_profit_pct = float(take_profit_pct)
        self.stop_loss_pct = float(stop_loss_pct)
        self.stop_loss_max = float(stop_loss_max)
        self.wingsize = float(wingsize)
        self.min_entry_credit = float(min_entry_credit)
        self.quantity = int(quantity)
        self.contract_multiplier = int(contract_multiplier)
        self.entry_gate_disabled = bool(entry_gate_disabled)
        self.entry_gate_start_time = str(entry_gate_start_time or market_start_time)
        self.entry_gate_min_credit = float(entry_gate_min_credit) if entry_gate_min_credit is not None else float(min_entry_credit)
        self.entry_gate_time_tolerance_seconds = max(0.0, float(entry_gate_time_tolerance_seconds))
        self.timed_walkthrough_policy = timed_walkthrough_policy
        self.enable_break_even = bool(enable_break_even)
        self.exchange_timezone = ZoneInfo("America/New_York")

        self.active_structure = None
        self.current_structure_quotes: dict[str, float | None] = {}
        self.current_structure_quote_details: dict[str, StructureQuote] = {}
        self.pending_orders: list[str] = []
        self.pending_transition: str | None = None
        self.pending_context: dict[str, object] = {}
        self.last_open_orders: list[BrokerOrderStatus] = []
        self.last_positions: list[BrokerPosition] = []
        self.entry_attempted = False
        self.entry_blocked_for_day = False
        self.entry_start_gate_checked = False
        self.entry_gate_first_check_at: datetime | None = None
        self.realized_pnl_usd = 0.0
        self.exit_reason: str | None = None
        self.trade_sequence = 0
        self.active_trade_id: str | None = None
        self._tick_transition_event: str | None = None
        self._tick_transition_reason: str | None = None
        self.break_even_center_strike: float | None = None
        self.break_even_entry_credit: float | None = None
        self.entry_fill_confirmed_at: datetime | None = None
        self.break_even_fill_confirmed_at: datetime | None = None

        self.iron_fly_position = None
        self.credit_spread_position = None

        self.output.log(
            "info",
            "trading session created",
            state=self.state,
            trade_date=self.trade_date.isoformat(),
            market_start=self.market_start_time,
            market_end=self.market_end_time,
            entry_gate_start=self.entry_gate_start_time,
            entry_gate_min_credit=self.entry_gate_min_credit,
            entry_gate_time_tolerance_seconds=self.entry_gate_time_tolerance_seconds,
            timed_walkthrough=bool(self.timed_walkthrough_policy is not None),
        )

    def apply_event(self, event_name: str, *, reason: str, **fields) -> str:
        next_state = EVENT_TRANSITIONS.get((self.state, event_name))
        if next_state is None:
            self.output.log(
                "error",
                "invalid state transition event",
                state=self.state,
                event=event_name,
                reason=reason,
                **fields,
            )
            raise ValueError(f"Event '{event_name}' is not valid for state '{self.state}'")
        self._transition_to(next_state, event_name=event_name, reason=reason, **fields)
        return self.state

    def _transition_to(self, new_state: str, *, event_name: str, reason: str, **fields) -> None:
        previous_state = self.state
        self.state = new_state
        self._tick_transition_event = event_name
        self._tick_transition_reason = reason
        event_timestamp = self._event_timestamp(fields)
        self.output.log(
            "info",
            "state transition",
            from_state=previous_state,
            to_state=new_state,
            event=event_name,
            reason=reason,
            **fields,
        )
        self.output.record_session_action(
            timestamp=event_timestamp,
            source="session",
            stage="state",
            event=event_name,
            from_state=previous_state,
            to_state=new_state,
            reason=reason,
            **fields,
        )

    def on_tick(self, market_snapshot, broker_snapshot, now):
        """
        Main event loop.

        Event flow:
        1. Reconcile broker state first.
        2. Update state machine from confirmed fills/orders.
        3. Evaluate actions for current state.
        4. Emit zero or more orders.
        """

        self._tick_transition_event = None
        self._tick_transition_reason = None
        previous_structure = self._current_structure_name()
        previous_position_qty = self._current_position_qty()

        self._ensure_structure_from_snapshot(market_snapshot)
        self._update_structure_quotes(market_snapshot)
        self.output.record_broker_snapshot(broker_snapshot)
        self.reconcile_broker_state(broker_snapshot)

        if self.state == "FLAT":
            result = self.handle_flat(market_snapshot, now)
        elif self.state == "ENTRY_PENDING":
            result = self.handle_entry_pending(market_snapshot, now)
        elif self.state == "IRON_FLY_ACTIVE":
            result = self.handle_iron_fly_active(market_snapshot, now)
        elif self.state == "BE_EXIT_PENDING":
            result = self.handle_be_exit_pending(market_snapshot, now)
        elif self.state == "CREDIT_SPREAD_ACTIVE":
            result = self.handle_credit_spread_active(market_snapshot, now)
        elif self.state == "EXIT_PENDING":
            result = self.handle_exit_pending(market_snapshot, now)
        elif self.state == "DONE":
            result = []
        else:
            self.output.log("warning", "unknown state encountered", state=self.state)
            result = self.fail_safe_exit()

        self._record_daily_detail(
            market_snapshot=market_snapshot,
            broker_snapshot=broker_snapshot,
            now=now,
            previous_structure=previous_structure,
            previous_position_qty=previous_position_qty,
        )
        return result

    def _is_in_market_window(self, now: datetime) -> bool:
        current_hhmm = self._local_hhmm(now)
        return self.market_start_time <= current_hhmm < self.market_end_time

    def _encode_exit_reason(self, *, reason: str) -> str:
        text = str(reason).strip().lower()
        if "stop loss" in text:
            return "sl"
        if "take profit" in text:
            return "tp"
        if "market window ended" in text:
            return "time"
        return text.replace(" ", "_")

    def _current_structure_name(self) -> str:
        if self.active_structure is None:
            return "flat"
        return str(self.active_structure.request.structure_type)

    def _current_position_qty(self) -> int:
        if self.credit_spread_position is not None:
            return int(self.credit_spread_position.quantity)
        if self.iron_fly_position is not None:
            return int(self.iron_fly_position.quantity)
        return 0

    def _current_mark_price(self) -> float | None:
        if self.credit_spread_position is not None:
            return self._credit_spread_mark(self.credit_spread_position.side)
        if self.iron_fly_position is not None:
            return self._iron_fly_mark()
        return None

    def _current_entry_price(self) -> float | None:
        if self.credit_spread_position is not None:
            return float(self.credit_spread_position.entry_credit)
        if self.iron_fly_position is not None:
            return float(self.iron_fly_position.entry_credit)
        return None

    def _current_exit_price(self) -> float | None:
        if self.credit_spread_position is not None:
            return self._credit_spread_exit_debit(self.credit_spread_position.side)
        if self.iron_fly_position is not None:
            return self._iron_fly_exit_debit()
        return None

    def _current_unrealized_pnl_usd(self) -> float | None:
        if self.credit_spread_position is not None:
            exit_price = self._credit_spread_exit_debit(self.credit_spread_position.side)
            if exit_price is None:
                return None
            return self._credit_to_pnl(
                self.credit_spread_position.entry_credit,
                exit_price,
                self.credit_spread_position.quantity,
            )
        if self.iron_fly_position is not None:
            exit_price = self._iron_fly_exit_debit()
            if exit_price is None:
                return None
            return self._credit_to_pnl(
                self.iron_fly_position.entry_credit,
                exit_price,
                self.iron_fly_position.quantity,
            )
        return 0.0 if self.realized_pnl_usd else None

    def _position_event(self, *, previous_structure: str, previous_position_qty: int) -> str:
        current_structure = self._current_structure_name()
        current_position_qty = self._current_position_qty()
        if previous_position_qty == 0 and current_position_qty > 0:
            return "open"
        if previous_position_qty > 0 and current_position_qty == 0:
            return "close"
        if current_position_qty > 0 and previous_structure != current_structure:
            return "structure_change"
        if current_position_qty < previous_position_qty:
            return "reduce"
        return "hold"

    def _tick_note(self, broker_snapshot) -> str | None:
        if self._tick_transition_event is not None:
            return self._tick_transition_event
        fills = getattr(broker_snapshot, "fills", None)
        if isinstance(fills, list) and fills:
            return ",".join(sorted({str(fill.purpose) for fill in fills}))
        return None

    def _record_daily_detail(
        self,
        *,
        market_snapshot,
        broker_snapshot,
        now: datetime,
        previous_structure: str,
        previous_position_qty: int,
    ) -> None:
        in_window = self._is_in_market_window(now)
        unrealized_pnl = self._current_unrealized_pnl_usd()
        realized_pnl = float(self.realized_pnl_usd)
        if unrealized_pnl is None:
            total_pnl = realized_pnl if realized_pnl != 0.0 else None
        else:
            total_pnl = realized_pnl + unrealized_pnl
        quality_counts = self._market_quality_counts(market_snapshot)
        metadata = getattr(market_snapshot, "metadata", {}) or {}
        be_lo, be_hi = self._current_break_even_bounds()
        leg_metrics = self._tick_leg_metrics()
        self.output.record_daily_detail(
            DailyDetailRow(
                ts=now,
                date=self.trade_date,
                und_px=getattr(market_snapshot, "underlying_price", None),
                state=self.state,
                struct=self._current_structure_name(),
                qty=self._current_position_qty(),
                pos_evt=self._position_event(
                    previous_structure=previous_structure,
                    previous_position_qty=previous_position_qty,
                ),
                exit_rsn=self.exit_reason,
                mark_px=self._current_mark_price(),
                entry_px=self._current_entry_price(),
                exit_px=self._current_exit_price(),
                u_pnl=unrealized_pnl,
                r_pnl=realized_pnl if realized_pnl != 0.0 else None,
                t_pnl=total_pnl,
                open_ords=len(self.last_open_orders),
                fills=len(getattr(broker_snapshot, "fills", []) or []),
                opt_q=int(metadata.get("option_quote_count") or 0),
                opt_q_all=int(metadata.get("all_option_quote_count") or 0),
                q_live=quality_counts["live"],
                q_ffill=quality_counts["ffill"],
                q_untrust=quality_counts["untrusted"],
                q_miss=quality_counts["missing"],
                note=self._tick_note(broker_snapshot),
                mark_pts=self._current_mark_price(),
                be_lo=be_lo,
                be_hi=be_hi,
                ph_n=1,
                sl_base=self.stop_loss_max,
                sl_eff=self._effective_stop_loss_usd(),
                tp_live=self._effective_take_profit_usd(),
                sl_eff_pct=self.stop_loss_pct,
                tp_live_pct=self.take_profit_pct,

                sc_k=leg_metrics["sc_k"],
                sc_m=leg_metrics["sc_m"],
                sp_k=leg_metrics["sp_k"],
                sp_m=leg_metrics["sp_m"],
                lc_k=leg_metrics["lc_k"],
                lc_m=leg_metrics["lc_m"],
                lp_k=leg_metrics["lp_k"],
                lp_m=leg_metrics["lp_m"],
                cum_b=total_pnl,
                cum_n=total_pnl,
            )
        )

    def _current_break_even_bounds(self) -> tuple[float | None, float | None]:
        center_strike = self.break_even_center_strike
        entry_credit = self.break_even_entry_credit
        if center_strike is None or entry_credit is None:
            if self.iron_fly_position is None:
                return None, None
            center_strike = self.iron_fly_position.center_strike
            entry_credit = self.iron_fly_position.entry_credit
        lower_be = float(center_strike - entry_credit)
        upper_be = float(center_strike + entry_credit)
        return lower_be, upper_be

    def _effective_take_profit_usd(self) -> float | None:
        entry_credit = self._current_entry_price()
        if entry_credit is not None:
            return entry_credit * self.contract_multiplier * self.take_profit_pct / 100.0
        return None

    def _effective_stop_loss_usd(self) -> float | None:
        entry_credit = self._current_entry_price()
        if entry_credit is not None:
            sl_from_pct = -(entry_credit * self.contract_multiplier * self.stop_loss_pct / 100.0)
            base_sl = max(sl_from_pct, float(self.stop_loss_max))
        else:
            base_sl = float(self.stop_loss_max)
        return base_sl

    def _tick_leg_metrics(self) -> dict[str, float | None]:
        return {
            "sc_k": self._tick_leg_strike("short_call"),
            "sc_m": self._tick_leg_mid("short_call"),
            "sp_k": self._tick_leg_strike("short_put"),
            "sp_m": self._tick_leg_mid("short_put"),
            "lc_k": self._tick_leg_strike("long_call"),
            "lc_m": self._tick_leg_mid("long_call"),
            "lp_k": self._tick_leg_strike("long_put"),
            "lp_m": self._tick_leg_mid("long_put"),
        }

    def _tick_leg_strike(self, label: str) -> float | None:
        if self.active_structure is None:
            return None
        leg_map = {leg.label: leg for leg in self.active_structure.request.legs}
        leg = leg_map.get(label)
        if leg is None:
            return None
        try:
            return float(getattr(leg, "strike", None))
        except Exception:
            return None

    def _tick_leg_mid(self, label: str) -> float | None:
        if self.active_structure is None:
            return None
        quote = self._quote_detail_for_label(label)
        if quote is not None and getattr(quote, "mid", None) is not None:
            return float(quote.mid)
        raw_value = self.current_structure_quotes.get(label)
        if raw_value is not None:
            return float(raw_value)
        return None

    def _market_quality_counts(self, market_snapshot) -> dict[str, int]:
        counts = {
            "live": 0,
            "ffill": 0,
            "untrusted": 0,
            "missing": 0,
        }
        metadata = getattr(market_snapshot, "metadata", {}) or {}
        option_quotes = metadata.get("option_quotes")
        if not isinstance(option_quotes, list):
            return counts
        for quote in option_quotes:
            if not isinstance(quote, dict):
                continue
            quality = str(quote.get("quality") or "missing").strip().lower()
            if quality in {"stale_ffill", "stale-ffill", "staleffill"}:
                quality = "ffill"
            if quality in counts:
                counts[quality] += 1
            else:
                counts["missing"] += 1
        return counts

    def _local_hhmm(self, now: datetime) -> str:
        if now.tzinfo is None:
            return now.strftime("%H:%M")
        return now.astimezone(self.exchange_timezone).strftime("%H:%M")

    def _is_after_market_end(self, now: datetime) -> bool:
        return self._local_hhmm(now) >= self.market_end_time

    def _has_complete_quotes(self) -> bool:
        return bool(self.current_structure_quotes) and all(value is not None for value in self.current_structure_quotes.values())

    def _price_to_pnl(self, entry_mark: float, current_mark: float, quantity: int | None = None) -> float:
        position_size = quantity or self.quantity
        return (entry_mark - current_mark) * self.contract_multiplier * position_size

    def _credit_to_pnl(self, entry_credit: float, exit_debit: float, quantity: int | None = None) -> float:
        position_size = quantity or self.quantity
        return (entry_credit - exit_debit) * self.contract_multiplier * position_size

    def _iron_fly_mark(self, quotes: dict[str, float | None] | None = None) -> float | None:
        values = quotes or self.current_structure_quotes
        try:
            short_call = values["short_call"]
            short_put = values["short_put"]
            long_call = values["long_call"]
            long_put = values["long_put"]
        except KeyError:
            return None
        if None in (short_call, short_put, long_call, long_put):
            return None
        return float(short_call) + float(short_put) - float(long_call) - float(long_put)

    def _credit_spread_mark(self, side: str, quotes: dict[str, float | None] | None = None) -> float | None:
        values = quotes or self.current_structure_quotes
        if side == "call":
            legs = (values.get("short_call"), values.get("long_call"))
        else:
            legs = (values.get("short_put"), values.get("long_put"))
        if None in legs:
            return None
        return float(legs[0]) - float(legs[1])

    def _quote_detail_for_label(self, label: str) -> StructureQuote | None:
        return self.current_structure_quote_details.get(label)

    def _entry_price_for_label(self, label: str) -> float | None:
        quote = self._quote_detail_for_label(label)
        if quote is None:
            value = self.current_structure_quotes.get(label)
            return float(value) if value is not None else None
        if label.startswith("short_"):
            if quote.bid is not None:
                return quote.bid
        elif label.startswith("long_"):
            if quote.ask is not None:
                return quote.ask
        if quote.mid is not None:
            return quote.mid
        if quote.bid is not None and quote.ask is not None:
            return (quote.bid + quote.ask) / 2.0
        return quote.bid if quote.bid is not None else quote.ask

    def _exit_price_for_label(self, label: str) -> float | None:
        quote = self._quote_detail_for_label(label)
        if quote is None:
            value = self.current_structure_quotes.get(label)
            return float(value) if value is not None else None
        if label.startswith("short_"):
            if quote.ask is not None:
                return quote.ask
        elif label.startswith("long_"):
            if quote.bid is not None:
                return quote.bid
        if quote.mid is not None:
            return quote.mid
        if quote.bid is not None and quote.ask is not None:
            return (quote.bid + quote.ask) / 2.0
        return quote.ask if quote.ask is not None else quote.bid

    def _structure_entry_credit(self, labels: tuple[str, ...]) -> float | None:
        prices: dict[str, float] = {}
        for label in labels:
            price = self._entry_price_for_label(label)
            if price is None:
                return None
            prices[label] = price
        credit = 0.0
        for label, price in prices.items():
            credit += price if label.startswith("short_") else -price
        return credit

    def _structure_exit_debit(self, labels: tuple[str, ...]) -> float | None:
        prices: dict[str, float] = {}
        for label in labels:
            price = self._exit_price_for_label(label)
            if price is None:
                return None
            prices[label] = price
        debit = 0.0
        for label, price in prices.items():
            debit += price if label.startswith("short_") else -price
        return debit

    def _iron_fly_entry_credit(self) -> float | None:
        return self._structure_entry_credit(("short_call", "short_put", "long_call", "long_put"))

    def _iron_fly_exit_debit(self) -> float | None:
        return self._structure_exit_debit(("short_call", "short_put", "long_call", "long_put"))

    def _credit_spread_entry_credit(self, side: str) -> float | None:
        if side == "call":
            return self._structure_entry_credit(("short_call", "long_call"))
        return self._structure_entry_credit(("short_put", "long_put"))

    def _credit_spread_exit_debit(self, side: str) -> float | None:
        if side == "call":
            return self._structure_exit_debit(("short_call", "long_call"))
        return self._structure_exit_debit(("short_put", "long_put"))

    def _entry_bracket(self, entry_credit: float) -> BracketSpec:
        effective_tp = entry_credit * self.contract_multiplier * self.take_profit_pct / 100.0
        take_profit_price = max(0.0, entry_credit - (effective_tp / self.contract_multiplier))
        sl_from_pct = -(entry_credit * self.contract_multiplier * self.stop_loss_pct / 100.0)
        effective_sl = max(sl_from_pct, float(self.stop_loss_max))
        stop_loss_price = entry_credit + (abs(effective_sl) / self.contract_multiplier)
        use_market = self.timed_walkthrough_policy is not None
        return BracketSpec(
            entry_limit_price=None if use_market else entry_credit,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
        )

    def _current_iron_fly_spec(self) -> IronFlySpec:
        if self.active_structure is None:
            raise ValueError("active structure is not initialized")

        leg_map = {leg.label: leg for leg in self.active_structure.request.legs}
        return IronFlySpec(
            underlying=self.active_structure.request.underlying,
            expiry=leg_map["short_call"].expiry.replace("-", ""),
            quantity=self.quantity,
            short_call_strike=leg_map["short_call"].strike,
            short_put_strike=leg_map["short_put"].strike,
            long_call_strike=leg_map["long_call"].strike,
            long_put_strike=leg_map["long_put"].strike,
        )

    def _build_credit_spread_request(self, side: str) -> StructureMarketDataRequest:
        if self.active_structure is None:
            raise ValueError("active structure is not initialized")

        leg_map = {leg.label: leg for leg in self.active_structure.request.legs}
        if side == "call":
            legs = (leg_map["short_call"], leg_map["long_call"])
        else:
            legs = (leg_map["short_put"], leg_map["long_put"])
        return StructureMarketDataRequest(
            structure_type="credit_spread",
            underlying=self.active_structure.request.underlying,
            legs=legs,
        )

    def _build_reduction_request(self, remaining_side: str) -> StructureMarketDataRequest:
        if self.active_structure is None:
            raise ValueError("active structure is not initialized")

        leg_map = {leg.label: leg for leg in self.active_structure.request.legs}
        if remaining_side == "call":
            legs = (leg_map["short_put"], leg_map["long_put"])
        else:
            legs = (leg_map["short_call"], leg_map["long_call"])
        return StructureMarketDataRequest(
            structure_type="reduction",
            underlying=self.active_structure.request.underlying,
            legs=legs,
        )

    def _timed_walkthrough_be_due(self, now: datetime) -> bool:
        if self.timed_walkthrough_policy is None:
            return False
        if self.entry_fill_confirmed_at is None:
            return False
        if self.pending_transition is not None:
            return False
        return (now - self.entry_fill_confirmed_at).total_seconds() >= float(self.timed_walkthrough_policy.be_after_seconds)

    def _timed_walkthrough_exit_due(self, now: datetime) -> bool:
        if self.timed_walkthrough_policy is None:
            return False
        if self.break_even_fill_confirmed_at is None:
            return False
        if self.pending_transition is not None:
            return False
        return (now - self.break_even_fill_confirmed_at).total_seconds() >= float(self.timed_walkthrough_policy.exit_after_seconds)

    def _submit_break_even_reduction(
        self,
        *,
        market_snapshot,
        now: datetime,
        executable_pnl: float,
        mark_pnl: float,
        reason: str,
    ) -> None:
        if self.iron_fly_position is None:
            return

        remaining_side = "call" if (market_snapshot.underlying_price or 0.0) >= self.iron_fly_position.center_strike else "put"
        remaining_request = self._build_credit_spread_request(remaining_side)
        remaining_quotes = {
            label: self.current_structure_quotes.get(label)
            for label in ("short_call", "long_call") if remaining_side == "call"
        } if remaining_side == "call" else {
            label: self.current_structure_quotes.get(label)
            for label in ("short_put", "long_put")
        }
        spread_mark = self._credit_spread_mark(remaining_side, remaining_quotes)
        if spread_mark is None:
            return
        spread_entry_credit = self._credit_spread_entry_credit(remaining_side)
        if spread_entry_credit is None:
            return

        reduction_request = self._build_reduction_request(remaining_side)
        combo_contract = self.broker.build_combo_contract_for_structure(
            structure=reduction_request,
            quantity=self.quantity,
        )
        order = self.broker.build_exit_order_for_combo(
            combo_contract=combo_contract,
            purpose="break_even_reduction",
            limit_price=None,
        )
        order_id = self.broker.submit_order(combo_contract=combo_contract, order=order)
        self.output.record_order_event(
            timestamp=now,
            event_type="order_submitted",
            order_id=order_id,
            trade_id=self.active_trade_id,
            source="session",
            state=self.state,
            structure="reduction",
            purpose="break_even_reduction",
            quantity=self.quantity,
            remaining_side=remaining_side,
            realized_pnl_usd=round(executable_pnl, 2),
        )
        if reason.startswith("timed walkthrough"):
            self.output.log(
                "info",
                "timed walkthrough trigger",
                stage="break_even",
                at=now.isoformat(),
            )

        self.pending_transition = "break_even_fill_confirmed"
        self.pending_orders = [order_id]
        self.pending_context = {
            "remaining_side": remaining_side,
            "remaining_request": remaining_request,
            "remaining_entry_mark": spread_mark,
            "remaining_entry_credit": spread_entry_credit,
            "realized_pnl_usd": executable_pnl,
            "reduction_request": reduction_request,
            "order_id": order_id,
        }
        self.apply_event(
            "break_even_triggered",
            reason=reason,
            at=now.isoformat(),
            pnl=round(executable_pnl, 2),
            mark_pnl=round(mark_pnl, 2),
            remaining_side=remaining_side,
            order_id=order_id,
        )

    def _begin_exit(self, *, reason: str, pnl: float, now: datetime, **fields) -> None:
        if self.active_structure is None:
            return
        self.exit_reason = self._encode_exit_reason(reason=reason)
        combo_contract = self.broker.build_combo_contract_for_structure(
            structure=self.active_structure.request,
            quantity=self.quantity,
        )
        order = self.broker.build_exit_order_for_combo(
            combo_contract=combo_contract,
            purpose="structure_exit",
            limit_price=None,
        )
        order_id = self.broker.submit_order(combo_contract=combo_contract, order=order)
        self.output.record_order_event(
            timestamp=now,
            event_type="order_submitted",
            order_id=order_id,
            trade_id=self.active_trade_id,
            source="session",
            state=self.state,
            structure=self._current_structure_name(),
            purpose="structure_exit",
            reason=reason,
            quantity=self.quantity,
        )
        self.pending_orders = [order_id]
        self.pending_transition = "exit_fill_confirmed"
        self.pending_context = {
            "reason": reason,
            "pnl": pnl,
            "timestamp": now.isoformat(),
            "order_id": order_id,
            **fields,
        }
        self.apply_event("exit_triggered", reason=reason, pnl=round(pnl, 2), at=now.isoformat(), order_id=order_id, **fields)

    @staticmethod
    def _event_timestamp(fields: dict[str, object]) -> datetime | None:
        raw_value = fields.get("at")
        if isinstance(raw_value, datetime):
            return raw_value
        if isinstance(raw_value, str):
            try:
                return datetime.fromisoformat(raw_value)
            except ValueError:
                return None
        return None

    def _next_trade_id(self) -> str:
        self.trade_sequence += 1
        return f"{self.trade_date.strftime('%Y%m%d')}_trade_{self.trade_sequence:04d}"

    def _ensure_structure_from_snapshot(self, market_snapshot) -> None:
        if self.state == "DONE":
            return

        if self.active_structure is not None:
            self.broker.ensure_structure_market_data(self.active_structure.request)
            return

        if market_snapshot.underlying_price is None:
            return

        center_strike = self._nearest_5_strike(market_snapshot.underlying_price)
        expiry = self.trade_date.isoformat()
        request = StructureMarketDataRequest(
            structure_type="iron_fly",
            underlying="SPX",
            legs=(
                StructureLeg(label="short_call", right="C", strike=center_strike, expiry=expiry),
                StructureLeg(label="short_put", right="P", strike=center_strike, expiry=expiry),
                StructureLeg(label="long_call", right="C", strike=center_strike + self.wingsize, expiry=expiry),
                StructureLeg(label="long_put", right="P", strike=center_strike - self.wingsize, expiry=expiry),
            ),
        )
        self.active_structure = SessionStructure(
            request=request,
            quote_labels=tuple(leg.label for leg in request.legs),
        )
        self.broker.ensure_structure_market_data(request)
        self.output.log(
            "info",
            "session structure initialized",
            trade_date=self.trade_date.isoformat(),
            structure_type=request.structure_type,
            center_strike=center_strike,
            expiry=expiry,
            legs=list(request.legs),
        )
        self.output.record_session_action(
            timestamp=getattr(market_snapshot, "timestamp", None),
            source="session",
            stage="setup",
            event="session_structure_initialized",
            trade_date=self.trade_date.isoformat(),
            structure_type=request.structure_type,
            center_strike=center_strike,
            expiry=expiry,
        )

    def _update_structure_quotes(self, market_snapshot) -> None:
        if self.active_structure is None:
            self.current_structure_quotes = {}
            self.current_structure_quote_details = {}
            return

        quotes = {label: market_snapshot.structure_quotes.get(label) for label in self.active_structure.quote_labels}
        self.current_structure_quotes = quotes
        details = getattr(market_snapshot, "structure_quote_details", {}) or {}
        self.current_structure_quote_details = {
            label: details.get(label, StructureQuote(label=label, bid=value, ask=value, mid=value))
            for label, value in quotes.items()
        }

        complete_quotes = sum(1 for value in quotes.values() if value is not None)
        self.output.log(
            "debug",
            "structure quotes updated",
            trade_date=self.trade_date.isoformat(),
            state=self.state,
            structure_type=self.active_structure.request.structure_type,
            quote_count=complete_quotes,
            total_legs=len(quotes),
        )

    @staticmethod
    def _nearest_5_strike(price: float) -> float:
        return round(price / 5.0) * 5.0

    def _evaluate_market_start_entry_gate(self, now: datetime) -> None:
        """Evaluate one-time entry gate at session start time.
        
        If entry gate is disabled (--no-entry-gate), skip all checks.
        Otherwise, check if minimum credit is available after the configured grace period.
        Once passed or blocked for the day, no further evaluation occurs.
        """
        if self.entry_gate_disabled:
            self.entry_start_gate_checked = True
            return
        if self.entry_start_gate_checked:
            return
        if self._local_hhmm(now) < self.entry_gate_start_time:
            return

        if self.entry_gate_first_check_at is None:
            self.entry_gate_first_check_at = now
        
        gate_elapsed_seconds = max(0.0, (now - self.entry_gate_first_check_at).total_seconds())
        gate_timeout_reached = gate_elapsed_seconds >= self.entry_gate_time_tolerance_seconds

        # Check 1: Are quotes available?
        if not self._has_complete_quotes():
            if not gate_timeout_reached:
                # Within grace period: wait for quotes to arrive
                return
            # Timeout reached: block day if quotes still unavailable
            self.entry_start_gate_checked = True
            self.entry_blocked_for_day = True
            self.output.log(
                "info",
                "entry disabled for day",
                reason="start-time gate could not be evaluated without complete quotes",
                gate_start=self.entry_gate_start_time,
                gate_elapsed_seconds=round(gate_elapsed_seconds, 2),
                entry_gate_time_tolerance_seconds=self.entry_gate_time_tolerance_seconds,
            )
            return

        # Check 2: Can we calculate entry credit?
        entry_credit = self._iron_fly_entry_credit()
        if entry_credit is None:
            if not gate_timeout_reached:
                # Within grace period: wait for credit calculation
                return
            # Timeout reached: block day if credit cannot be calculated
            self.entry_start_gate_checked = True
            self.entry_blocked_for_day = True
            self.output.log(
                "info",
                "entry disabled for day",
                reason="start-time entry credit unavailable",
                gate_start=self.entry_gate_start_time,
                gate_elapsed_seconds=round(gate_elapsed_seconds, 2),
                entry_gate_time_tolerance_seconds=self.entry_gate_time_tolerance_seconds,
            )
            return

        # Check 3: Is credit sufficient?
        entry_credit_usd = entry_credit * self.contract_multiplier
        if entry_credit_usd < self.entry_gate_min_credit:
            if not gate_timeout_reached:
                # Within grace period: wait for credit to improve
                return
            # Timeout reached: block day if credit still insufficient
            self.entry_start_gate_checked = True
            self.entry_blocked_for_day = True
            self.output.log(
                "info",
                "entry disabled for day",
                reason="start-time minimum entry credit not met",
                gate_start=self.entry_gate_start_time,
                entry_credit_usd=round(entry_credit_usd, 2),
                min_entry_credit=self.entry_gate_min_credit,
                gate_elapsed_seconds=round(gate_elapsed_seconds, 2),
                entry_gate_time_tolerance_seconds=self.entry_gate_time_tolerance_seconds,
            )
            return

        # All checks passed: entry gate is satisfied, trading can begin
        self.entry_start_gate_checked = True
        self.output.log(
            "debug",
            "entry gate passed at market start",
            gate_start=self.entry_gate_start_time,
            entry_credit_usd=round(entry_credit_usd, 2),
            min_entry_credit=self.entry_gate_min_credit,
            gate_elapsed_seconds=round(gate_elapsed_seconds, 2),
            entry_gate_time_tolerance_seconds=self.entry_gate_time_tolerance_seconds,
        )

    def handle_flat(self, market_snapshot, now):
        if self._tick_transition_event == "entry_rejected":
            return []

        self._evaluate_market_start_entry_gate(now)
        if not self.entry_gate_disabled and self._local_hhmm(now) < self.entry_gate_start_time:
            return []
        if self.entry_blocked_for_day:
            return []

        if not self._has_complete_quotes():
            return []
        if not self.entry_gate_disabled and not self._is_in_market_window(now):
            return []
        if self.entry_attempted:
            return []

        entry_mark = self._iron_fly_mark()
        entry_credit = self._iron_fly_entry_credit()
        if entry_mark is None or entry_credit is None:
            return []

        entry_credit_usd = entry_credit * self.contract_multiplier
        if not self.entry_gate_disabled and entry_credit_usd < self.entry_gate_min_credit:
            # Credit insufficient: entry gate must fully evaluate before allowing entry attempts.
            # The gate evaluation (in _evaluate_market_start_entry_gate) will decide if we're
            # still within the grace period (waiting for credit to improve) or past it (blocked for day).
            if not self.entry_start_gate_checked:
                # Gate still evaluating: during grace period, do not log repeatedly
                return []
            # Gate evaluation complete (either passed or blocked): at this point credit < min means day-blocked
            return []

        combo_contract = self.broker.build_iron_fly_combo_contract(self._current_iron_fly_spec())
        bracket_spec = self._entry_bracket(entry_credit)
        bracket_orders = self.broker.build_entry_bracket_for_combo(
            combo_contract=combo_contract,
            bracket=bracket_spec,
        )
        broker_order_ids = self.broker.submit_bracket(
            combo_contract=combo_contract,
            bracket_orders=bracket_orders,
        )
        trade_id = self._next_trade_id()
        order_purposes = ("entry", "take_profit", "stop_loss")
        for index, order_id in enumerate(broker_order_ids):
            self.output.record_order_event(
                timestamp=now,
                event_type="order_submitted",
                order_id=order_id,
                trade_id=trade_id,
                parent_order_id=broker_order_ids[0] if index > 0 and broker_order_ids else None,
                source="session",
                state=self.state,
                structure="iron_fly",
                purpose=order_purposes[index] if index < len(order_purposes) else "submitted",
                quantity=self.quantity,
                entry_mark=round(entry_mark, 4),
                entry_credit=round(entry_credit, 4),
            )
        self.pending_orders = broker_order_ids[:1]
        self.pending_transition = "entry_fill_confirmed"
        self.pending_context = {
            "entry_mark": entry_mark,
            "entry_credit": entry_credit,
            "center_strike": self._nearest_5_strike(market_snapshot.underlying_price),
            "wingsize": self.wingsize,
            "order_id": broker_order_ids[0],
            "trade_id": trade_id,
        }
        self.entry_attempted = True
        self.apply_event(
            "entry_signal",
            reason="whole iron fly entry submitted",
            at=now.isoformat(),
            entry_mark=round(entry_mark, 4),
            entry_credit=round(entry_credit, 4),
            order_count=len(broker_order_ids),
        )
        return []

    def handle_entry_pending(self, market_snapshot, now):
        return []

    def handle_iron_fly_active(self, market_snapshot, now):
        if self.iron_fly_position is None or not self._has_complete_quotes():
            return []

        current_mark = self._iron_fly_mark()
        exit_debit = self._iron_fly_exit_debit()
        if current_mark is None or exit_debit is None:
            return []

        mark_pnl = self._price_to_pnl(self.iron_fly_position.entry_mark, current_mark, self.iron_fly_position.quantity)
        executable_pnl = self._credit_to_pnl(
            self.iron_fly_position.entry_credit,
            exit_debit,
            self.iron_fly_position.quantity,
        )
        if not self._is_in_market_window(now):
            self._begin_exit(
                reason="market window ended for iron fly",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        if self._has_open_protection_orders("iron_fly"):
            return []

        if self.enable_break_even and self._timed_walkthrough_be_due(now):
            self._submit_break_even_reduction(
                market_snapshot=market_snapshot,
                now=now,
                executable_pnl=executable_pnl,
                mark_pnl=mark_pnl,
                reason="timed walkthrough break-even trigger reached",
            )
            return []

        if executable_pnl <= self._effective_stop_loss_usd():
            self._begin_exit(
                reason="iron fly stop loss reached",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        if executable_pnl >= self._effective_take_profit_usd():
            self._begin_exit(
                reason="iron fly take profit reached",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        return []

    def handle_be_exit_pending(self, market_snapshot, now):
        return []

    def handle_credit_spread_active(self, market_snapshot, now):
        if self.credit_spread_position is None or not self._has_complete_quotes():
            return []

        current_mark = self._credit_spread_mark(self.credit_spread_position.side)
        exit_debit = self._credit_spread_exit_debit(self.credit_spread_position.side)
        if current_mark is None or exit_debit is None:
            return []

        mark_pnl = self._price_to_pnl(self.credit_spread_position.entry_mark, current_mark, self.credit_spread_position.quantity)
        executable_pnl = self._credit_to_pnl(
            self.credit_spread_position.entry_credit,
            exit_debit,
            self.credit_spread_position.quantity,
        )
        if self._timed_walkthrough_exit_due(now):
            self.output.log(
                "info",
                "timed walkthrough trigger",
                stage="exit",
                at=now.isoformat(),
            )
            self._begin_exit(
                reason="timed walkthrough exit reached",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        if not self._is_in_market_window(now):
            self._begin_exit(
                reason="market window ended for credit spread",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        if executable_pnl <= self._effective_stop_loss_usd():
            self._begin_exit(
                reason="credit spread stop loss reached",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        if executable_pnl >= self._effective_take_profit_usd():
            self._begin_exit(
                reason="credit spread take profit reached",
                pnl=executable_pnl,
                now=now,
                mark_pnl=mark_pnl,
                exit_debit=exit_debit,
            )
            return []

        return []

    def handle_exit_pending(self, market_snapshot, now):
        return []

    def reconcile_broker_state(self, broker_snapshot):
        self.last_open_orders = list(broker_snapshot.open_orders)
        self.last_positions = list(broker_snapshot.positions)

        if self.pending_transition and self.pending_orders:
            matching_fill = self._matching_pending_fill(broker_snapshot.fills)
            if matching_fill is None:
                rejected_order = self._matching_pending_terminal_reject()
                if self.pending_transition == "entry_fill_confirmed" and rejected_order is not None:
                    self._reject_pending_entry(rejected_order, broker_snapshot.timestamp)
                    return
                if self.pending_transition == "entry_fill_confirmed" and self._position_for_type("iron_fly") is not None:
                    self._confirm_entry_fill(None, broker_snapshot.timestamp)
                    return
                if self.pending_transition == "break_even_fill_confirmed" and self._position_for_type("credit_spread") is not None:
                    self._confirm_break_even_fill(None, broker_snapshot.timestamp)
                    return
                if self.pending_transition == "exit_fill_confirmed" and self._current_position() is None and not self._has_pending_open_orders():
                    self._confirm_exit_fill(None, broker_snapshot.timestamp)
                return
            if self.pending_transition == "entry_fill_confirmed":
                self._confirm_entry_fill(matching_fill, broker_snapshot.timestamp)
                return
            if self.pending_transition == "break_even_fill_confirmed":
                self._confirm_break_even_fill(matching_fill, broker_snapshot.timestamp)
                return
            if self.pending_transition == "exit_fill_confirmed":
                self._confirm_exit_fill(matching_fill, broker_snapshot.timestamp)
                return

        exit_fill = self._broker_managed_exit_fill(broker_snapshot.fills)
        if exit_fill is not None and self.state in {"IRON_FLY_ACTIVE", "CREDIT_SPREAD_ACTIVE"}:
            self._finalize_broker_managed_exit(exit_fill, broker_snapshot.timestamp)

    def _matching_pending_fill(self, fills: list[BrokerFill]) -> BrokerFill | None:
        pending = set(self.pending_orders)
        for fill in fills:
            if fill.order_id in pending and fill.status == "filled":
                return fill
        return None

    def _matching_pending_terminal_reject(self) -> BrokerOrderStatus | None:
        pending = set(self.pending_orders)
        for order in self.last_open_orders:
            if order.order_id in pending and order.status == "rejected":
                return order
        return None

    def _has_pending_open_orders(self) -> bool:
        pending = set(self.pending_orders)
        if not pending:
            return False
        for order in self.last_open_orders:
            if order.order_id in pending and order.status in {"submitted", "held"}:
                return True
        return False

    def _broker_managed_exit_fill(self, fills: list[BrokerFill]) -> BrokerFill | None:
        expected_structure_type = "iron_fly" if self.state == "IRON_FLY_ACTIVE" else "credit_spread"
        for fill in fills:
            if fill.status != "filled":
                continue
            if fill.purpose not in {"take_profit", "stop_loss", "structure_exit"}:
                continue
            if fill.structure_type == expected_structure_type:
                return fill
        return None

    def _position_for_type(self, structure_type: str) -> BrokerPosition | None:
        for position in self.last_positions:
            if position.structure_type == structure_type and position.quantity > 0:
                return position
        return None

    def _current_position(self) -> BrokerPosition | None:
        if self.state in {"IRON_FLY_ACTIVE", "ENTRY_PENDING"}:
            return self._position_for_type("iron_fly")
        if self.state in {"CREDIT_SPREAD_ACTIVE", "BE_EXIT_PENDING", "EXIT_PENDING"}:
            position = self._position_for_type("credit_spread")
            if position is not None:
                return position
            return self._position_for_type("iron_fly")
        return None

    def _has_open_protection_orders(self, structure_type: str) -> bool:
        for order in self.last_open_orders:
            if order.structure_type == structure_type and order.purpose in {"take_profit", "stop_loss"} and order.status == "submitted":
                return True
        return False

    def _confirm_entry_fill(self, fill: BrokerFill | None, now: datetime) -> None:
        self.exit_reason = None
        trade_id = str(self.pending_context.get("trade_id") or self._next_trade_id())
        fill_credit = float(fill.fill_price) if fill is not None and fill.fill_price is not None else float(self.pending_context["entry_credit"])
        # IBKR combo entry fills can arrive as signed SELL prices (negative credit).
        # The session model stores credit as a positive value for PnL/TP/SL math.
        fill_credit = abs(fill_credit)
        self.iron_fly_position = IronFlyPosition(
            entry_mark=float(self.pending_context["entry_mark"]),
            entry_credit=fill_credit,
            quantity=self.quantity,
            center_strike=float(self.pending_context["center_strike"]),
            wingsize=float(self.pending_context["wingsize"]),
        )
        self.break_even_center_strike = self.iron_fly_position.center_strike
        self.break_even_entry_credit = self.iron_fly_position.entry_credit
        self.entry_fill_confirmed_at = now
        self.break_even_fill_confirmed_at = None
        self.active_trade_id = trade_id
        self.pending_orders = []
        self.pending_transition = None
        self.pending_context = {}
        self.apply_event(
            "entry_fill_confirmed",
            reason="broker confirmed full iron fly entry fill",
            at=now.isoformat(),
            entry_mark=round(self.iron_fly_position.entry_mark, 4),
            entry_credit=round(self.iron_fly_position.entry_credit, 4),
            broker_order_id=fill.order_id if fill is not None else None,
        )
        self.output.record_trade_event(
            timestamp=now,
            event_type="trade_opened",
            trade_id=trade_id,
            order_id=fill.order_id if fill is not None else None,
            source="session",
            state=self.state,
            structure="iron_fly",
            entry_mark=round(self.iron_fly_position.entry_mark, 4),
            entry_credit=round(self.iron_fly_position.entry_credit, 4),
            quantity=self.quantity,
            center_strike=self.iron_fly_position.center_strike,
            wingsize=self.iron_fly_position.wingsize,
        )

    def _reject_pending_entry(self, order: BrokerOrderStatus, now: datetime) -> None:
        order_id = str(self.pending_context.get("order_id") or order.order_id)
        self.pending_orders = []
        self.pending_transition = None
        self.pending_context = {}
        self.output.log(
            "warning",
            "broker rejected iron fly entry order",
            at=now.isoformat(),
            broker_order_id=order_id,
            status=order.status,
        )
        self.apply_event(
            "entry_rejected",
            reason="broker rejected full iron fly entry order",
            at=now.isoformat(),
            broker_order_id=order_id,
            status=order.status,
        )

    def _confirm_break_even_fill(self, fill: BrokerFill | None, now: datetime) -> None:
        remaining_request = self.pending_context.get("remaining_request")
        if not isinstance(remaining_request, StructureMarketDataRequest):
            return
        trade_id = self.active_trade_id
        self.realized_pnl_usd = float(self.pending_context.get("realized_pnl_usd", self.realized_pnl_usd))
        self.active_structure = SessionStructure(
            request=remaining_request,
            quote_labels=tuple(leg.label for leg in remaining_request.legs),
        )
        self.credit_spread_position = CreditSpreadPosition(
            side=str(self.pending_context["remaining_side"]),
            entry_mark=float(self.pending_context["remaining_entry_mark"]),
            entry_credit=float(self.pending_context["remaining_entry_credit"]),
            quantity=self.quantity,
        )
        self.iron_fly_position = None
        self.broker.ensure_structure_market_data(remaining_request)
        remaining_side = str(self.pending_context["remaining_side"])
        remaining_entry_mark = float(self.pending_context["remaining_entry_mark"])
        remaining_entry_credit = float(self.pending_context["remaining_entry_credit"])
        self.break_even_fill_confirmed_at = now
        self.pending_orders = []
        self.pending_transition = None
        self.pending_context = {}
        self.apply_event(
            "break_even_fill_confirmed",
            reason="broker confirmed break-even reduction fill",
            at=now.isoformat(),
            remaining_side=remaining_side,
            entry_mark=round(remaining_entry_mark, 4),
            entry_credit=round(remaining_entry_credit, 4),
            broker_order_id=fill.order_id if fill is not None else None,
            reduction_fill_price=fill.fill_price if fill is not None else None,
        )
        if trade_id is not None:
            self.output.record_trade_event(
                timestamp=now,
                event_type="trade_updated",
                trade_id=trade_id,
                order_id=fill.order_id if fill is not None else None,
                source="session",
                state=self.state,
                structure="credit_spread",
                remaining_side=remaining_side,
                entry_mark=round(remaining_entry_mark, 4),
                entry_credit=round(remaining_entry_credit, 4),
                realized_pnl_usd=round(self.realized_pnl_usd, 2),
                reduction_fill_price=fill.fill_price if fill is not None else None,
            )

    def _confirm_exit_fill(self, fill: BrokerFill | None, now: datetime) -> None:
        exit_reason = str(self.pending_context.get("reason", "structure exit confirmed"))
        trade_id = self.active_trade_id
        pnl = self._realized_pnl_from_fill(fill)
        if self.credit_spread_position is not None and self.realized_pnl_usd:
            pnl += self.realized_pnl_usd
        self.realized_pnl_usd = pnl
        self.active_structure = None
        self.current_structure_quotes = {}
        self.current_structure_quote_details = {}
        self.pending_orders = []
        self.pending_transition = None
        self.pending_context = {}
        self.iron_fly_position = None
        self.credit_spread_position = None
        self.break_even_center_strike = None
        self.break_even_entry_credit = None
        self.entry_fill_confirmed_at = None
        self.break_even_fill_confirmed_at = None
        self.apply_event(
            "exit_fill_confirmed",
            reason=exit_reason,
            at=now.isoformat(),
            pnl=round(pnl, 2),
            broker_order_id=fill.order_id if fill is not None else None,
            exit_fill_price=fill.fill_price if fill is not None else None,
        )
        if trade_id is not None:
            self.output.record_trade_event(
                timestamp=now,
                event_type="trade_closed",
                trade_id=trade_id,
                order_id=fill.order_id if fill is not None else None,
                source="session",
                state=self.state,
                structure="flat",
                exit_reason=self.exit_reason,
                realized_pnl_usd=round(pnl, 2),
                exit_fill_price=fill.fill_price if fill is not None else None,
            )
        self.active_trade_id = None

    def _realized_pnl_from_fill(self, fill: BrokerFill | None) -> float:
        if fill is None or fill.fill_price is None:
            return float(self.pending_context.get("pnl", 0.0))
        if self.credit_spread_position is not None:
            return self._credit_to_pnl(
                self.credit_spread_position.entry_credit,
                float(fill.fill_price),
                self.credit_spread_position.quantity,
            )
        if self.iron_fly_position is not None:
            return self._credit_to_pnl(
                self.iron_fly_position.entry_credit,
                float(fill.fill_price),
                self.iron_fly_position.quantity,
            )
        return float(self.pending_context.get("pnl", 0.0))

    def _finalize_broker_managed_exit(self, fill: BrokerFill, now: datetime) -> None:
        reason_map = {
            "take_profit": "broker take profit filled",
            "stop_loss": "broker stop loss filled",
            "structure_exit": "broker structure exit filled",
        }
        reason = reason_map.get(fill.purpose, "broker managed exit filled")
        self.exit_reason = self._encode_exit_reason(reason=reason)
        self.apply_event(
            "exit_triggered",
            reason=reason,
            at=now.isoformat(),
            broker_order_id=fill.order_id,
            broker_managed=True,
        )
        self.pending_transition = "exit_fill_confirmed"
        self.pending_context = {
            "reason": reason,
            "pnl": self._realized_pnl_from_fill(fill),
            "timestamp": now.isoformat(),
        }
        self.pending_orders = []
        self._confirm_exit_fill(fill, now)

    def fail_safe_exit(self):
        # Logic:
        # - flatten all remaining exposure
        # - mark strategy as terminal / error state
        self.output.log("error", "fail safe exit requested", state=self.state)
        return []


# Suggested state vocabulary:
# - FLAT
# - ENTRY_PENDING
# - IRON_FLY_ACTIVE
# - BE_EXIT_PENDING
# - CREDIT_SPREAD_ACTIVE
# - EXIT_PENDING
# - DONE
# - ERROR


# Suggested event vocabulary:
# - entry_signal
# - iron_fly_entry_filled
# - iron_fly_tp_hit
# - iron_fly_sl_hit
# - break_even_triggered
# - break_even_reduction_filled
# - spread_tp_hit
# - spread_sl_hit
# - exit_filled
# - reject_or_inconsistent_fill


# Suggested structure abstraction:
#
# IronFlyPosition:
# - entry_credit
# - quantity
# - four legs
# - current_combo_mark
# - unrealized_pnl
# - realized_pnl
#
# CreditSpreadPosition:
# - side (call spread or put spread)
# - entry_credit_after_be
# - quantity
# - two remaining legs
# - current_combo_mark
# - unrealized_pnl
# - realized_pnl