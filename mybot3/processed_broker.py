"""Processed broker placeholder for the mybot3 protocol."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from mybot3.broker import (
    BrokerFill,
    BrokerOrderStatus,
    BrokerPosition,
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
    StructureQuote,
    StructureMarketDataRequest,
    build_combo_contract_from_structure,
    build_combo_order,
    build_entry_bracket_orders,
    build_fill_metadata_for_structure,
    build_iron_fly_structure_request,
    coerce_number,
    coerce_scalar,
    normalize_expiry_value,
    normalize_expiry_token,
    preferred_option_price,
    register_submitted_runtime_order,
    resolve_order_purpose,
)


@dataclass(frozen=True)
class ProcessedReplayInput:
    trade_date: date
    index_file: Path
    options_file: Path


@dataclass
class ProcessedBroker(MyBot3BrokerProtocol):
    """
    Local replay broker for processed market data.

    This broker creates deterministic placeholder payloads so TradingSession can
    work against the same protocol in replay mode.
    """

    source: str = "processed"
    fill_model: str = "midpoint"
    replay_inputs: list[ProcessedReplayInput] | None = None
    requested_structure: StructureMarketDataRequest | None = None
    order_sequence: int = 1
    runtime: ComboOrderRuntime = field(init=False)
    active_orders: dict[str, dict[str, object]] = field(default_factory=dict)
    active_positions: dict[str, BrokerPosition] = field(default_factory=dict)

    _PRICE_EPSILON = 1e-9

    def __post_init__(self) -> None:
        self.runtime = ComboOrderRuntime(source=self.source)
        self.active_orders = self.runtime.order_states  # compatibility alias
        self.active_positions = self.runtime.active_positions

    @staticmethod
    def available_sources(processed_root: str | Path) -> list[str]:
        root = Path(processed_root)
        if not root.exists():
            return []
        return sorted(path.name for path in root.iterdir() if path.is_dir())

    @classmethod
    def from_date_range(
        cls,
        *,
        source: str,
        processed_root: str | Path,
        start_date: date,
        end_date: date,
        fill_model: str = "midpoint",
        skip_missing: bool = False,
    ) -> "ProcessedBroker":
        replay_inputs = cls.resolve_replay_inputs(
            processed_root=processed_root,
            source=source,
            start_date=start_date,
            end_date=end_date,
            skip_missing=skip_missing,
        )
        return cls(source=source, fill_model=fill_model, replay_inputs=replay_inputs)

    @classmethod
    def from_market_dir(
        cls,
        *,
        market_dir: str | Path,
        start_date: date,
        end_date: date,
        fill_model: str = "midpoint",
        source: str = "market_dir",
    ) -> "ProcessedBroker":
        replay_inputs = cls.resolve_replay_inputs_from_market_dir(
            market_dir=market_dir,
            start_date=start_date,
            end_date=end_date,
        )
        return cls(source=source, fill_model=fill_model, replay_inputs=replay_inputs)

    @classmethod
    def resolve_replay_inputs(
        cls,
        *,
        processed_root: str | Path,
        source: str,
        start_date: date,
        end_date: date,
        skip_missing: bool = False,
    ) -> list[ProcessedReplayInput]:
        resolved_inputs: list[ProcessedReplayInput] = []
        for trade_date in cls._iter_trade_dates(start_date, end_date):
            try:
                index_file = cls._resolve_processed_file(processed_root, source=source, kind="index", trade_date=trade_date)
                options_file = cls._resolve_processed_file(processed_root, source=source, kind="options", trade_date=trade_date)
            except ValueError:
                if skip_missing:
                    continue
                raise
            resolved_inputs.append(
                ProcessedReplayInput(
                    trade_date=trade_date,
                    index_file=index_file,
                    options_file=options_file,
                )
            )
        return resolved_inputs

    @classmethod
    def resolve_replay_inputs_from_market_dir(
        cls,
        *,
        market_dir: str | Path,
        start_date: date,
        end_date: date,
    ) -> list[ProcessedReplayInput]:
        resolved_inputs: list[ProcessedReplayInput] = []
        for trade_date in cls._iter_trade_dates(start_date, end_date):
            index_file = cls._resolve_market_dir_file(market_dir, kind="index", trade_date=trade_date)
            options_file = cls._resolve_market_dir_file(market_dir, kind="options", trade_date=trade_date)
            resolved_inputs.append(
                ProcessedReplayInput(
                    trade_date=trade_date,
                    index_file=index_file,
                    options_file=options_file,
                )
            )
        return resolved_inputs

    def trade_dates(self) -> list[date]:
        return [item.trade_date for item in self.replay_inputs or []]

    @staticmethod
    def _iter_trade_dates(start_date: date, end_date: date):
        current = start_date
        while current <= end_date:
            yield current
            current = current.fromordinal(current.toordinal() + 1)

    @staticmethod
    def _candidate_priority(path: Path, *, kind: str) -> tuple[int, str]:
        name = path.name.lower()
        if kind == "options":
            if "cbbo" in name:
                return (0, name)
            if "cmbp" in name:
                return (1, name)
            if "quotes_raw" in name:
                return (2, name)
            return (3, name)
        if "ohlcv" in name:
            return (0, name)
        if "quotes_raw" in name:
            return (1, name)
        return (2, name)

    @classmethod
    def _resolve_processed_file(
        cls,
        processed_root: str | Path,
        *,
        source: str,
        kind: str,
        trade_date: date,
    ) -> Path:
        root = Path(processed_root)
        symbol_dir = "spx" if kind == "index" else "spxw"
        search_root = root / source / symbol_dir / f"{trade_date.year}"
        date_token = trade_date.strftime("%Y%m%d")
        candidates = sorted(search_root.glob(f"*_{date_token}.processed.parquet"))
        if not candidates:
            raise ValueError(
                f"No {kind} processed file found for source '{source}' on {trade_date.isoformat()} in {search_root}"
            )
        if len(candidates) == 1:
            return candidates[0]

        ranked = sorted(candidates, key=lambda path: cls._candidate_priority(path, kind=kind))
        best = ranked[0]
        best_priority = cls._candidate_priority(best, kind=kind)
        competing = [path for path in ranked if cls._candidate_priority(path, kind=kind) == best_priority]
        if len(competing) > 1:
            candidate_text = ", ".join(path.name for path in candidates)
            raise ValueError(
                f"Multiple equally preferred {kind} processed files found for source '{source}' on {trade_date.isoformat()}: {candidate_text}"
            )
        return best

    @classmethod
    def _resolve_market_dir_file(
        cls,
        market_dir: str | Path,
        *,
        kind: str,
        trade_date: date,
    ) -> Path:
        root = Path(market_dir)
        date_token = trade_date.strftime("%Y%m%d")
        all_candidates = sorted(root.glob(f"*_{date_token}.processed.parquet"))
        if kind == "index":
            candidates = [path for path in all_candidates if cls._is_index_market_file(path)]
        else:
            candidates = [path for path in all_candidates if cls._is_options_market_file(path)]
        if not candidates:
            raise ValueError(
                f"No {kind} processed file found on {trade_date.isoformat()} in market dir {root}"
            )
        if len(candidates) == 1:
            return candidates[0]

        ranked = sorted(candidates, key=lambda path: cls._candidate_priority(path, kind=kind))
        best = ranked[0]
        best_priority = cls._candidate_priority(best, kind=kind)
        competing = [path for path in ranked if cls._candidate_priority(path, kind=kind) == best_priority]
        if len(competing) > 1:
            candidate_text = ", ".join(path.name for path in candidates)
            raise ValueError(
                f"Multiple equally preferred {kind} processed files found on {trade_date.isoformat()} in market dir {root}: {candidate_text}"
            )
        return best

    @staticmethod
    def _is_index_market_file(path: Path) -> bool:
        name = path.name.lower()
        return "spxw" not in name and "spx" in name

    @staticmethod
    def _is_options_market_file(path: Path) -> bool:
        name = path.name.lower()
        return "spxw" in name

    def build_iron_fly_combo_contract(self, spec: IronFlySpec) -> ComboContract:
        structure_request = build_iron_fly_structure_request(spec)
        return build_combo_contract_from_structure(
            broker="processed",
            structure_request=structure_request,
            quantity=spec.quantity,
            extra_fields={
                "source": self.source,
                "fill_model": self.fill_model,
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
            broker="processed",
            structure_request=structure,
            quantity=quantity,
            extra_fields={
                "source": self.source,
                "fill_model": self.fill_model,
            },
        )

    def build_entry_bracket_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        bracket: BracketSpec,
    ) -> BracketOrderSet:
        return build_entry_bracket_orders(
            broker="processed",
            combo_contract=combo_contract,
            bracket=bracket,
            extra_fields={
                "fill_model": self.fill_model,
            },
        )

    def build_exit_order_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        purpose: str,
        limit_price: float | None,
    ) -> BrokerOrder:
        return build_combo_order(
            broker="processed",
            combo_contract=combo_contract,
            purpose=purpose,
            order_type="LMT" if limit_price is not None else "MKT",
            limit_price=limit_price,
            extra_fields={
                "fill_model": self.fill_model,
            },
        )

    def submit_bracket(
        self,
        *,
        combo_contract: ComboContract,
        bracket_orders: BracketOrderSet,
    ) -> list[str]:
        parent_id = self._register_order(
            combo_contract=combo_contract,
            order=bracket_orders.parent,
            purpose="entry",
        )
        tp_id = self._register_order(
            combo_contract=combo_contract,
            order=bracket_orders.take_profit,
            purpose="take_profit",
            parent_order_id=parent_id,
        )
        sl_id = self._register_order(
            combo_contract=combo_contract,
            order=bracket_orders.stop_loss,
            purpose="stop_loss",
            parent_order_id=parent_id,
        )
        return [parent_id, tp_id, sl_id]

    def submit_order(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
    ) -> str:
        purpose = resolve_order_purpose(order)
        return self._register_order(combo_contract=combo_contract, order=order, purpose=purpose)

    def ensure_structure_market_data(self, structure: StructureMarketDataRequest) -> None:
        self.requested_structure = structure

    def iter_session_updates(self, *, trade_date: date):
        replay_input = self._replay_input_for_date(trade_date)
        index_df = self._read_index_frame(replay_input.index_file)
        options_groups = self._read_options_groups(replay_input.options_file)

        self._reset_runtime_state()

        for row in index_df.itertuples(index=False):
            event_time = row.timestamp.to_pydatetime()
            all_option_quotes = options_groups.get(row.timestamp, [])
            option_quotes = self._filter_option_quotes(all_option_quotes)
            structure_quotes = self._build_structure_quotes(option_quotes)
            structure_quote_details = self._build_structure_quote_details(option_quotes)
            fills, open_orders, positions = self._process_orders(event_time, structure_quotes, structure_quote_details)
            yield SessionUpdate(
                timestamp=event_time,
                market_snapshot=MarketSnapshot(
                    timestamp=event_time,
                    underlying_price=coerce_number(row.last),
                    structure_quotes=structure_quotes,
                    source=self.source,
                    metadata={
                        "trade_date": trade_date.isoformat(),
                        "index_file": str(replay_input.index_file),
                        "options_file": str(replay_input.options_file),
                        "fill_model": self.fill_model,
                        "mode": "processed_replay",
                        "index_row": {
                            "symbol": coerce_scalar(getattr(row, "symbol", None)),
                            "last": coerce_number(getattr(row, "last", None)),
                            "last_adjusted": coerce_number(getattr(row, "last_adjusted", None)),
                            "spot_from_options": coerce_number(getattr(row, "spot_from_options", None)),
                            "source": coerce_scalar(getattr(row, "source", None)),
                        },
                        "requested_structure": self.requested_structure,
                        "all_option_quote_count": len(all_option_quotes),
                        "option_quote_count": len(option_quotes),
                        "option_quotes": option_quotes,
                    },
                    structure_quote_details=structure_quote_details,
                ),
                broker_snapshot=BrokerSnapshot(
                    timestamp=event_time,
                    open_orders=open_orders,
                    fills=fills,
                    positions=positions,
                    metadata={
                        "broker": "processed",
                        "trade_date": trade_date.isoformat(),
                        "source": self.source,
                    },
                ),
            )

    def _replay_input_for_date(self, trade_date: date) -> ProcessedReplayInput:
        for replay_input in self.replay_inputs or []:
            if replay_input.trade_date == trade_date:
                return replay_input
        raise ValueError(f"No processed replay input configured for {trade_date.isoformat()}")

    def _filter_option_quotes(self, option_quotes: list[dict[str, object]]) -> list[dict[str, object]]:
        if self.requested_structure is None:
            return option_quotes

        by_key = {
            (str(quote.get("right", "")).upper(), float(quote.get("strike")), normalize_expiry_value(quote.get("expiry"))): quote
            for quote in option_quotes
            if quote.get("right") is not None and quote.get("strike") is not None and quote.get("expiry") is not None
        }

        filtered: list[dict[str, object]] = []
        for leg in self.requested_structure.legs:
            quote = by_key.get((leg.right.upper(), float(leg.strike), leg.expiry))
            if quote is None:
                filtered.append(
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
                        "stale_seconds": None,
                        "instrument_id": None,
                    }
                )
                continue
            filtered.append({"label": leg.label, **quote})
        return filtered

    _TRUSTED_QUOTE_QUALITIES = frozenset({"live", "stale_ffill", "stale-ffill", "staleffill"})

    @classmethod
    def _is_trusted_quote(cls, quote: dict[str, object]) -> bool:
        quality = str(quote.get("quality") or "missing").strip().lower()
        return quality in cls._TRUSTED_QUOTE_QUALITIES

    def _build_structure_quotes(self, option_quotes: list[dict[str, object]]) -> dict[str, float | None]:
        structure_quotes: dict[str, float | None] = {}
        for quote in option_quotes:
            label = quote.get("label")
            if not isinstance(label, str):
                continue
            if self._is_trusted_quote(quote):
                price = preferred_option_price(quote)
            else:
                price = None
            structure_quotes[label] = price
        return structure_quotes

    def _build_structure_quote_details(self, option_quotes: list[dict[str, object]]) -> dict[str, StructureQuote]:
        structure_quote_details: dict[str, StructureQuote] = {}
        for quote in option_quotes:
            label = quote.get("label")
            if not isinstance(label, str):
                continue
            if self._is_trusted_quote(quote):
                structure_quote_details[label] = StructureQuote(
                    label=label,
                    bid=coerce_number(quote.get("bid")),
                    ask=coerce_number(quote.get("ask")),
                    mid=coerce_number(quote.get("mid")),
                )
            else:
                structure_quote_details[label] = StructureQuote(
                    label=label,
                    bid=None,
                    ask=None,
                    mid=None,
                )
        return structure_quote_details

    def _register_order(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
        purpose: str,
        parent_order_id: str | None = None,
    ) -> str:
        order_id = f"processed-{purpose}-{self.order_sequence}"
        self.order_sequence += 1
        return register_submitted_runtime_order(
            self.runtime,
            order_id=order_id,
            combo_contract=combo_contract,
            order=order,
            purpose=purpose,
            parent_order_id=parent_order_id,
        )

    def _reset_runtime_state(self) -> None:
        self.runtime.reset()
        self.active_orders = self.runtime.order_states
        self.active_positions = self.runtime.active_positions

    def _process_orders(
        self,
        event_time: datetime,
        structure_quotes: dict[str, float | None],
        structure_quote_details: dict[str, StructureQuote],
    ) -> tuple[list[BrokerFill], list[BrokerOrderStatus], list[BrokerPosition]]:
        fills: list[BrokerFill] = []
        for order_state in list(self.runtime.iter_order_states()):
            if order_state.status != "submitted":
                continue
            if order_state.activated_at == event_time:
                continue
            purpose = str(order_state.purpose)
            structure_request = order_state.structure_request
            if not isinstance(structure_request, StructureMarketDataRequest):
                continue

            fill_price = self._resolve_fill_price(
                structure_request=structure_request,
                purpose=purpose,
                order=order_state.order,
                structure_quotes=structure_quotes,
                structure_quote_details=structure_quote_details,
            )
            if fill_price is None:
                continue

            order_state.order.raw["fill_metadata"] = self._fill_metadata_for_structure(structure_request, purpose, structure_quote_details)
            fill = self.runtime.apply_fill(
                order_id=order_state.order_id,
                fill_price=fill_price,
                timestamp=event_time,
                metadata={
                    "simulated": True,
                    "fill_model": self.fill_model,
                },
            )
            if fill is not None:
                fills.append(fill)

        open_orders = self.runtime.open_order_statuses()
        positions = self.runtime.positions()
        return fills, open_orders, positions

    def _resolve_fill_price(
        self,
        *,
        structure_request: StructureMarketDataRequest,
        purpose: str,
        order: BrokerOrder,
        structure_quotes: dict[str, float | None],
        structure_quote_details: dict[str, StructureQuote],
    ) -> float | None:
        if purpose == "entry":
            mark_price = self._structure_mark(structure_request, structure_quotes)
            if mark_price is None:
                return None
            limit_price = coerce_number(order.raw.get("lmtPrice"))
            if limit_price is not None and mark_price + self._PRICE_EPSILON < limit_price:
                return None
            return limit_price if limit_price is not None else mark_price

        if purpose == "take_profit":
            mark_price = self._structure_mark(structure_request, structure_quotes)
            trigger_price = coerce_number(order.raw.get("lmtPrice"))
            if mark_price is None or trigger_price is None or mark_price - self._PRICE_EPSILON > trigger_price:
                return None
            return self._structure_exit_debit(structure_request, structure_quote_details)

        if purpose == "stop_loss":
            mark_price = self._structure_mark(structure_request, structure_quotes)
            trigger_price = coerce_number(order.raw.get("auxPrice"))
            if mark_price is None or trigger_price is None or mark_price + self._PRICE_EPSILON < trigger_price:
                return None
            return self._structure_exit_debit(structure_request, structure_quote_details)

        executable_price = self._structure_exit_debit(structure_request, structure_quote_details)
        if executable_price is None:
            return None
        limit_price = coerce_number(order.raw.get("lmtPrice"))
        if limit_price is not None and executable_price - self._PRICE_EPSILON > limit_price:
            return None
        return executable_price

    def _structure_mark(
        self,
        structure_request: StructureMarketDataRequest,
        structure_quotes: dict[str, float | None],
    ) -> float | None:
        values = [structure_quotes.get(leg.label) for leg in structure_request.legs]
        if any(value is None for value in values):
            return None
        total = 0.0
        for leg, value in zip(structure_request.legs, values):
            sign = -1.0 if leg.label.startswith("long_") else 1.0
            total += sign * float(value)
        return total

    def _structure_exit_debit(
        self,
        structure_request: StructureMarketDataRequest,
        structure_quote_details: dict[str, StructureQuote],
    ) -> float | None:
        total = 0.0
        for leg in structure_request.legs:
            quote = structure_quote_details.get(leg.label)
            if quote is None:
                return None
            if leg.label.startswith("short_"):
                price = quote.ask if quote.ask is not None else quote.mid
            else:
                price = quote.bid if quote.bid is not None else quote.mid
            if price is None:
                return None
            total += price if leg.label.startswith("short_") else -price
        return total

    def _fill_metadata_for_structure(
        self,
        structure_request: StructureMarketDataRequest,
        purpose: str,
        structure_quote_details: dict[str, StructureQuote],
    ) -> dict[str, object]:
        return build_fill_metadata_for_structure(
            structure_request,
            purpose=purpose,
            leg_fill_price_resolver=lambda leg, side: self._leg_fill_price(
                quote=structure_quote_details[leg.label],
                side=side,
            ) if leg.label in structure_quote_details else None,
        )

    def _leg_fill_price(self, *, quote: StructureQuote, side: str) -> float | None:
        if side == "BOT":
            return quote.ask if quote.ask is not None else quote.mid
        return quote.bid if quote.bid is not None else quote.mid

    def _apply_fill_to_positions(
        self,
        *,
        structure_request: StructureMarketDataRequest,
        purpose: str,
        quantity: int,
        order: BrokerOrder,
    ) -> None:
        key = structure_request.structure_type
        if purpose == "entry":
            self.active_positions[key] = BrokerPosition(
                symbol=structure_request.underlying,
                structure_type=structure_request.structure_type,
                quantity=quantity,
                legs=tuple(leg.label for leg in structure_request.legs),
                metadata={"source": self.source},
            )
            return

        if purpose == "break_even_reduction":
            self.active_positions.pop("iron_fly", None)
            resulting_request = None
            if hasattr(order, "raw"):
                resulting_request = order.raw.get("resulting_structure_request")
            if isinstance(resulting_request, StructureMarketDataRequest):
                self.active_positions[resulting_request.structure_type] = BrokerPosition(
                    symbol=resulting_request.underlying,
                    structure_type=resulting_request.structure_type,
                    quantity=quantity,
                    legs=tuple(leg.label for leg in resulting_request.legs),
                    metadata={"source": self.source},
                )
            return

        self.active_positions.pop(key, None)
        if purpose in {"take_profit", "stop_loss", "structure_exit"}:
            self.active_positions.pop("iron_fly", None)
            self.active_positions.pop("credit_spread", None)

    def _handle_related_orders_after_fill(self, *, order_id: str, purpose: str, structure_type: str | None, event_time: datetime) -> None:
        if purpose != "entry":
            for candidate_id, candidate in self.active_orders.items():
                if candidate_id == order_id:
                    continue
                if structure_type is not None and candidate.get("structure_type") == structure_type and candidate["status"] in {"submitted", "held"}:
                    candidate["status"] = "cancelled"
            return
        for candidate_id, candidate in self.active_orders.items():
            if candidate.get("parent_order_id") == order_id:
                candidate["status"] = "submitted"
                candidate["activated_at"] = event_time

    def _open_order_statuses(self) -> list[BrokerOrderStatus]:
        statuses: list[BrokerOrderStatus] = []
        for order_id, order_state in self.active_orders.items():
            if order_state["status"] not in {"submitted", "held"}:
                continue
            statuses.append(
                BrokerOrderStatus(
                    order_id=order_id,
                    purpose=str(order_state["purpose"]),
                    status=str(order_state["status"]),
                    structure_type=order_state.get("structure_type"),
                    quantity=int(order_state["quantity"]),
                    filled_quantity=0,
                    remaining_quantity=int(order_state["quantity"]),
                    metadata={"source": self.source},
                )
            )
        return statuses

    def _read_index_frame(self, path: Path) -> pd.DataFrame:
        df = pd.read_parquet(path)
        if "timestamp" not in df.columns:
            raise ValueError(f"timestamp column missing in {path}")
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values("timestamp")
        return df

    def _read_options_groups(self, path: Path) -> dict[pd.Timestamp, list[dict[str, object]]]:
        df = pd.read_parquet(path)
        if "timestamp" not in df.columns:
            raise ValueError(f"timestamp column missing in {path}")
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if "expiry" in df.columns:
            df["expiry"] = pd.to_datetime(df["expiry"], utc=True, errors="coerce")
        df = df.dropna(subset=["timestamp"]).sort_values(["timestamp", "right", "strike"])

        grouped: dict[pd.Timestamp, list[dict[str, object]]] = {}
        for timestamp, frame in df.groupby("timestamp", sort=True):
            grouped[timestamp] = [self._normalize_option_row(record) for record in frame.to_dict("records")]
        return grouped

    def _normalize_option_row(self, record: dict[str, object]) -> dict[str, object]:
        normalized: dict[str, object] = {}
        for key, value in record.items():
            if key == "timestamp":
                continue
            if key == "expiry":
                normalized[key] = normalize_expiry_value(value)
                continue
            normalized[key] = coerce_scalar(value)
        return normalized
