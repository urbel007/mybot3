"""
Minimal broker protocol for mybot3 pseudocode.

Purpose:
- Define the smallest broker-facing interface needed by MyBot3StateMachine.
- Keep this file conceptual and lightweight.
- Focus first on what handle_flat(...) needs to create and submit a full Iron Fly entry bracket.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol, Any, Callable, Iterator, Mapping


@dataclass
class IronFlySpec:
    """
    Minimal description of the intended Iron Fly structure.

    Pseudocode payload only.
    A later implementation could replace Any with precise contract models.
    """

    underlying: str
    expiry: str
    quantity: int
    short_call_strike: float
    short_put_strike: float
    long_call_strike: float
    long_put_strike: float


@dataclass
class BracketSpec:
    """
    Minimal bracket pricing description for the whole combo.
    """

    entry_limit_price: float | None
    take_profit_price: float
    stop_loss_price: float


@dataclass
class ComboContract:
    """
    Placeholder for a broker-native combo contract, e.g. an IBKR BAG contract.
    """

    raw: Any


@dataclass
class BrokerOrder:
    """
    Placeholder for one broker order object.
    """

    raw: Any


@dataclass
class BracketOrderSet:
    """
    Full bracket for one combo entry.

    Expected shape:
    - parent entry order
    - take profit child order
    - stop loss child order
    """

    parent: BrokerOrder
    take_profit: BrokerOrder
    stop_loss: BrokerOrder


@dataclass(frozen=True)
class MarketSnapshot:
    timestamp: datetime
    underlying_price: float | None
    structure_quotes: dict[str, float | None]
    source: str
    metadata: dict[str, Any]
    structure_quote_details: dict[str, "StructureQuote"] = field(default_factory=dict)


@dataclass(frozen=True)
class StructureQuote:
    label: str
    bid: float | None
    ask: float | None
    mid: float | None


@dataclass(frozen=True)
class StructureLeg:
    label: str
    right: str
    strike: float
    expiry: str


@dataclass(frozen=True)
class StructureMarketDataRequest:
    structure_type: str
    underlying: str
    legs: tuple[StructureLeg, ...]


@dataclass(frozen=True)
class BrokerOrderStatus:
    order_id: str
    purpose: str
    status: str
    structure_type: str | None
    quantity: int
    filled_quantity: int
    remaining_quantity: int
    metadata: dict[str, Any]


@dataclass(frozen=True)
class BrokerFill:
    order_id: str
    purpose: str
    status: str
    structure_type: str | None
    quantity: int
    fill_price: float | None
    metadata: dict[str, Any]


@dataclass(frozen=True)
class BrokerPosition:
    symbol: str
    structure_type: str
    quantity: int
    legs: tuple[str, ...]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class BrokerSnapshot:
    timestamp: datetime
    open_orders: list[BrokerOrderStatus]
    fills: list[BrokerFill]
    positions: list[BrokerPosition]
    metadata: dict[str, Any]


@dataclass(frozen=True)
class SessionUpdate:
    timestamp: datetime
    market_snapshot: MarketSnapshot
    broker_snapshot: BrokerSnapshot


@dataclass
class RuntimeOrderState:
    order_id: str
    purpose: str
    status: str
    activated_at: datetime | None
    combo_contract: ComboContract
    order: BrokerOrder
    structure_request: StructureMarketDataRequest | None
    structure_type: str | None
    quantity: int
    parent_order_id: str | None
    filled_quantity: int = 0
    remaining_quantity: int = 0
    avg_fill_price: float | None = None


def normalize_broker_order_status(status: str | None, *, parent_order_id: str | None = None) -> str:
    normalized = str(status or "").strip().lower().replace("_", " ")
    if normalized in {"filled"}:
        return "filled"
    if normalized in {"inactive"}:
        return "rejected"
    if normalized in {"cancelled", "canceled", "api cancelled"}:
        return "cancelled"
    if normalized in {"rejected"}:
        return "rejected"
    if normalized in {"held"}:
        return "held"
    if parent_order_id is not None and normalized in {"pending submit", "presubmitted"}:
        return "held"
    if normalized:
        return "submitted"
    return "held" if parent_order_id is not None else "submitted"


class ComboOrderRuntime:
    def __init__(self, *, source: str) -> None:
        self.source = source
        self.order_states: dict[str, RuntimeOrderState] = {}
        self.active_positions: dict[str, BrokerPosition] = {}

    def reset(self) -> None:
        self.order_states = {}
        self.active_positions = {}

    def register_order(
        self,
        *,
        order_id: str,
        combo_contract: ComboContract,
        order: BrokerOrder,
        purpose: str,
        parent_order_id: str | None = None,
        initial_status: str | None = None,
    ) -> str:
        structure_request = combo_contract.raw.get("structure_request")
        quantity = int(combo_contract.raw.get("structure", {}).get("quantity", 1))
        status = normalize_broker_order_status(initial_status, parent_order_id=parent_order_id)
        self.order_states[order_id] = RuntimeOrderState(
            order_id=order_id,
            purpose=purpose,
            status=status,
            activated_at=None,
            combo_contract=combo_contract,
            order=order,
            structure_request=structure_request if isinstance(structure_request, StructureMarketDataRequest) else None,
            structure_type=getattr(structure_request, "structure_type", None),
            quantity=quantity,
            parent_order_id=parent_order_id,
            filled_quantity=0,
            remaining_quantity=quantity,
            avg_fill_price=None,
        )
        return order_id

    def update_order_status(
        self,
        *,
        order_id: str,
        status: str | None,
        filled_quantity: int | float | None = None,
        remaining_quantity: int | float | None = None,
        avg_fill_price: float | None = None,
    ) -> None:
        state = self.order_states.get(order_id)
        if state is None:
            return
        state.status = normalize_broker_order_status(status, parent_order_id=state.parent_order_id)
        if filled_quantity is not None:
            state.filled_quantity = int(filled_quantity)
        if remaining_quantity is not None:
            state.remaining_quantity = int(remaining_quantity)
        if avg_fill_price is not None:
            state.avg_fill_price = float(avg_fill_price)

    def apply_fill(
        self,
        *,
        order_id: str,
        fill_price: float | None,
        timestamp: datetime,
        metadata: dict[str, Any] | None = None,
    ) -> BrokerFill | None:
        state = self.order_states.get(order_id)
        if state is None or state.status == "filled":
            return None

        state.status = "filled"
        state.filled_quantity = int(state.quantity)
        state.remaining_quantity = 0
        state.avg_fill_price = float(fill_price) if fill_price is not None else None

        structure_request = state.structure_request
        fill_metadata = state.order.raw.get("fill_metadata") if hasattr(state.order, "raw") else None
        if not isinstance(fill_metadata, dict):
            fill_metadata = build_fill_metadata_for_structure(structure_request, purpose=state.purpose)

        combined_metadata = {
            **fill_metadata,
            "source": self.source,
            "trade_date": timestamp.date().isoformat(),
            **(metadata or {}),
        }

        fill = BrokerFill(
            order_id=order_id,
            purpose=state.purpose,
            status="filled",
            structure_type=state.structure_type,
            quantity=int(state.quantity),
            fill_price=float(fill_price) if fill_price is not None else None,
            metadata=combined_metadata,
        )
        self._apply_fill_to_positions(state=state)
        self._handle_related_orders_after_fill(order_id=order_id, timestamp=timestamp)
        return fill

    def open_order_statuses(self) -> list[BrokerOrderStatus]:
        statuses: list[BrokerOrderStatus] = []
        for state in self.order_states.values():
            if state.status not in {"submitted", "held", "rejected"}:
                continue
            statuses.append(
                BrokerOrderStatus(
                    order_id=state.order_id,
                    purpose=state.purpose,
                    status=state.status,
                    structure_type=state.structure_type,
                    quantity=int(state.quantity),
                    filled_quantity=int(state.filled_quantity),
                    remaining_quantity=int(state.remaining_quantity),
                    metadata={"source": self.source},
                )
            )
        return statuses

    def positions(self) -> list[BrokerPosition]:
        return list(self.active_positions.values())

    def get_order_state(self, order_id: str) -> RuntimeOrderState | None:
        return self.order_states.get(order_id)

    def iter_order_states(self) -> Iterator[RuntimeOrderState]:
        return iter(self.order_states.values())

    def _apply_fill_to_positions(self, *, state: RuntimeOrderState) -> None:
        structure_request = state.structure_request
        if structure_request is None:
            return

        key = structure_request.structure_type
        if state.purpose == "entry":
            self.active_positions[key] = BrokerPosition(
                symbol=structure_request.underlying,
                structure_type=structure_request.structure_type,
                quantity=state.quantity,
                legs=tuple(leg.label for leg in structure_request.legs),
                metadata={"source": self.source},
            )
            return

        if state.purpose == "break_even_reduction":
            self.active_positions.pop("iron_fly", None)
            resulting_request = None
            if hasattr(state.order, "raw"):
                resulting_request = state.order.raw.get("resulting_structure_request")
            if isinstance(resulting_request, StructureMarketDataRequest):
                self.active_positions[resulting_request.structure_type] = BrokerPosition(
                    symbol=resulting_request.underlying,
                    structure_type=resulting_request.structure_type,
                    quantity=state.quantity,
                    legs=tuple(leg.label for leg in resulting_request.legs),
                    metadata={"source": self.source},
                )
            return

        self.active_positions.pop(key, None)
        if state.purpose in {"take_profit", "stop_loss", "structure_exit"}:
            self.active_positions.pop("iron_fly", None)
            self.active_positions.pop("credit_spread", None)

    def _handle_related_orders_after_fill(self, *, order_id: str, timestamp: datetime) -> None:
        state = self.order_states.get(order_id)
        if state is None:
            return
        if state.purpose != "entry":
            for candidate in self.order_states.values():
                if candidate.order_id == order_id:
                    continue
                if state.structure_type is not None and candidate.structure_type == state.structure_type and candidate.status in {"submitted", "held"}:
                    candidate.status = "cancelled"
            return
        for candidate in self.order_states.values():
            if candidate.parent_order_id == order_id and candidate.status == "held":
                candidate.status = "submitted"
                candidate.activated_at = timestamp


def normalize_expiry_token(value: str) -> str:
    if len(value) == 8 and value.isdigit():
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return value


def coerce_number(value: object) -> float | None:
    if value is None:
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def is_missing_value(value: object) -> bool:
    if value is None:
        return True
    value_type = type(value)
    if value_type.__name__ in {"NAType", "NaTType"}:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def coerce_scalar(value: object) -> object:
    if is_missing_value(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            return value
        if is_missing_value(value):
            return None
    return value


def normalize_expiry_value(value: object) -> str | None:
    value = coerce_scalar(value)
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def preferred_option_price(quote: Mapping[str, object]) -> float | None:
    mid = coerce_number(quote.get("mid"))
    if mid is not None:
        return mid
    bid = coerce_number(quote.get("bid"))
    ask = coerce_number(quote.get("ask"))
    if bid is not None and ask is not None:
        return (bid + ask) / 2.0
    return bid if bid is not None else ask


def build_iron_fly_structure_request(spec: IronFlySpec) -> StructureMarketDataRequest:
    expiry = normalize_expiry_token(spec.expiry)
    return StructureMarketDataRequest(
        structure_type="iron_fly",
        underlying=spec.underlying,
        legs=(
            StructureLeg(label="short_call", right="C", strike=spec.short_call_strike, expiry=expiry),
            StructureLeg(label="short_put", right="P", strike=spec.short_put_strike, expiry=expiry),
            StructureLeg(label="long_call", right="C", strike=spec.long_call_strike, expiry=expiry),
            StructureLeg(label="long_put", right="P", strike=spec.long_put_strike, expiry=expiry),
        ),
    )


def build_combo_contract_payload(
    *,
    broker: str,
    structure_request: StructureMarketDataRequest,
    quantity: int,
    extra_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "broker": broker,
        "structure_request": structure_request,
        "structure": {
            "underlying": structure_request.underlying,
            "quantity": int(quantity),
            "structure_type": structure_request.structure_type,
            "legs": list(structure_request.legs),
        },
    }
    if extra_fields:
        payload.update(dict(extra_fields))
    return payload


def build_combo_contract_from_structure(
    *,
    broker: str,
    structure_request: StructureMarketDataRequest,
    quantity: int,
    extra_fields: Mapping[str, object] | None = None,
) -> ComboContract:
    return ComboContract(
        raw=build_combo_contract_payload(
            broker=broker,
            structure_request=structure_request,
            quantity=quantity,
            extra_fields=extra_fields,
        )
    )


def build_combo_order_payload(
    *,
    broker: str,
    combo_contract: ComboContract,
    purpose: str,
    order_type: str,
    role: str | None = None,
    limit_price: float | None = None,
    stop_price: float | None = None,
    extra_fields: Mapping[str, object] | None = None,
) -> dict[str, object]:
    structure_request = combo_contract.raw.get("structure_request") if hasattr(combo_contract, "raw") else None
    normalized_order_type = str(order_type or "MKT").upper()
    payload: dict[str, object] = {
        "broker": broker,
        "combo_contract": combo_contract.raw,
        "purpose": purpose,
        "orderType": normalized_order_type,
        "fill_metadata": build_fill_metadata_for_structure(structure_request, purpose=purpose),
    }
    if role is not None:
        payload["role"] = role
    if normalized_order_type == "LMT" or limit_price is not None:
        payload["lmtPrice"] = limit_price
    if normalized_order_type == "STP" or stop_price is not None:
        payload["auxPrice"] = stop_price
    if extra_fields:
        payload.update(dict(extra_fields))
    return payload


def build_combo_order(
    *,
    broker: str,
    combo_contract: ComboContract,
    purpose: str,
    order_type: str,
    role: str | None = None,
    limit_price: float | None = None,
    stop_price: float | None = None,
    extra_fields: Mapping[str, object] | None = None,
) -> BrokerOrder:
    return BrokerOrder(
        raw=build_combo_order_payload(
            broker=broker,
            combo_contract=combo_contract,
            purpose=purpose,
            order_type=order_type,
            role=role,
            limit_price=limit_price,
            stop_price=stop_price,
            extra_fields=extra_fields,
        )
    )


def build_entry_bracket_orders(
    *,
    broker: str,
    combo_contract: ComboContract,
    bracket: BracketSpec,
    extra_fields: Mapping[str, object] | None = None,
) -> BracketOrderSet:
    parent_order_type = "LMT" if bracket.entry_limit_price is not None else "MKT"
    return BracketOrderSet(
        parent=build_combo_order(
            broker=broker,
            combo_contract=combo_contract,
            purpose="entry",
            role="parent",
            order_type=parent_order_type,
            limit_price=bracket.entry_limit_price,
            extra_fields=extra_fields,
        ),
        take_profit=build_combo_order(
            broker=broker,
            combo_contract=combo_contract,
            purpose="take_profit",
            role="take_profit",
            order_type="LMT",
            limit_price=bracket.take_profit_price,
            extra_fields=extra_fields,
        ),
        stop_loss=build_combo_order(
            broker=broker,
            combo_contract=combo_contract,
            purpose="stop_loss",
            role="stop_loss",
            order_type="STP",
            stop_price=bracket.stop_loss_price,
            extra_fields=extra_fields,
        ),
    )


def resolve_order_purpose(order: BrokerOrder, *, default: str = "structure_exit") -> str:
    raw = getattr(order, "raw", None)
    if isinstance(raw, Mapping):
        return str(raw.get("purpose") or default)
    return default


def register_submitted_runtime_order(
    runtime: ComboOrderRuntime,
    *,
    order_id: str,
    combo_contract: ComboContract,
    order: BrokerOrder,
    purpose: str,
    parent_order_id: str | None = None,
) -> str:
    runtime.register_order(
        order_id=order_id,
        combo_contract=combo_contract,
        order=order,
        purpose=purpose,
        parent_order_id=parent_order_id,
        initial_status="held" if parent_order_id is not None else "submitted",
    )
    return order_id


def leg_fill_side(*, leg: StructureLeg, purpose: str) -> str:
    if purpose == "entry":
        return "SLD" if leg.label.startswith("short_") else "BOT"
    return "BOT" if leg.label.startswith("short_") else "SLD"


def format_leg_instrument(underlying: str, leg: StructureLeg) -> str:
    expiry_text = leg.expiry
    try:
        if len(expiry_text) == 8 and expiry_text.isdigit():
            expiry_value = datetime.strptime(expiry_text, "%Y%m%d")
        else:
            expiry_value = datetime.fromisoformat(expiry_text)
        expiry_text = expiry_value.strftime("%b%d'%y")
    except ValueError:
        pass
    right_text = "CALL" if leg.right.upper() == "C" else "PUT"
    return f"{underlying} {expiry_text} {leg.strike:g} {right_text}"


def build_fill_metadata_for_structure(
    structure_request: object,
    *,
    purpose: str,
    leg_fill_price_resolver: Callable[[StructureLeg, str], float | None] | None = None,
) -> dict[str, object]:
    if not isinstance(structure_request, StructureMarketDataRequest):
        return {"purpose": purpose, "leg_fills": []}

    leg_fills = []
    for leg in structure_request.legs:
        side = leg_fill_side(leg=leg, purpose=purpose)
        fill_price = leg_fill_price_resolver(leg, side) if leg_fill_price_resolver is not None else None
        leg_fills.append(
            {
                "instrument": format_leg_instrument(structure_request.underlying, leg),
                "side": side,
                "fill_price": fill_price,
                "quantity": 1,
                "multiplier": 100,
            }
        )

    metadata: dict[str, object] = {
        "purpose": purpose,
        "structure_type": structure_request.structure_type,
        "underlying": structure_request.underlying,
        "legs": [leg.label for leg in structure_request.legs],
        "instrument": f"{structure_request.structure_type}:{','.join(leg.label for leg in structure_request.legs)}",
        "leg_fills": leg_fills,
    }
    if len(structure_request.legs) == 1:
        leg = structure_request.legs[0]
        metadata["side"] = "BOT" if leg.label.startswith("short_") else "SLD"
    else:
        metadata["side"] = "BOT" if purpose != "entry" else "SLD"
    return metadata


class MyBot3BrokerProtocol(Protocol):
    """
    Minimal protocol needed by MyBot3StateMachine.

    This is intentionally small.
    The first goal is only to support handle_flat(...).
    """

    def build_iron_fly_combo_contract(self, spec: IronFlySpec) -> ComboContract:
        """
        Build one broker-native combo contract for the whole Iron Fly.

        Example later implementation:
        - IBKR BAG contract
        - 4 combo legs
        - SMART routing or direct exchange routing
        """
        ...

    def build_entry_bracket_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        bracket: BracketSpec,
    ) -> BracketOrderSet:
        """
        Build the three broker orders needed for a full combo bracket.

        Expected intent:
        - parent entry order for the Iron Fly combo
        - take profit child order on the same combo
        - stop loss child order on the same combo
        """
        ...

    def submit_bracket(
        self,
        *,
        combo_contract: ComboContract,
        bracket_orders: BracketOrderSet,
    ) -> list[str]:
        """
        Submit the combo bracket to the broker.

        Returns:
        - list of broker order ids
        - normally [parent_id, tp_id, sl_id]
        """
        ...

    def ensure_structure_market_data(self, structure: StructureMarketDataRequest) -> None:
        """
        Ensure market-data subscriptions or replay bindings exist for a structure.

        Live brokers can start or reuse subscriptions here.
        Replay brokers can use this as a lightweight no-op.
        """
        ...

    def build_combo_contract_for_structure(
        self,
        *,
        structure: StructureMarketDataRequest,
        quantity: int,
    ) -> ComboContract:
        """
        Build one broker-native combo contract for an arbitrary structure.

        This is used for structure reductions and full exits after the initial
        Iron Fly entry.
        """
        ...

    def build_exit_order_for_combo(
        self,
        *,
        combo_contract: ComboContract,
        purpose: str,
        limit_price: float | None,
    ) -> BrokerOrder:
        """
        Build one combo exit order for either a reduction or a full close.

        Example purposes:
        - break_even_reduction
        - structure_exit
        """
        ...

    def submit_order(
        self,
        *,
        combo_contract: ComboContract,
        order: BrokerOrder,
    ) -> str:
        """
        Submit one standalone combo order to the broker.

        Returns the broker order id for later fill reconciliation.
        """
        ...

    def iter_session_updates(self, *, trade_date: date) -> Iterator[SessionUpdate]:
        """
        Yield session updates for one trading day.

        Each update should contain both current market data and current broker
        state so TradingSession can remain broker-agnostic.
        """
        ...


