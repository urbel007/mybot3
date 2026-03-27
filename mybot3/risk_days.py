from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from typing import Any, Iterable, Sequence
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_RISK_EVENTS_PATH = Path("data/metadata/riskevents/risk_events.json")
FOMC_HTML_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
FOMC_ICS_URL = "https://www.federalreserve.gov/feeds/meetingcalendar.ics"
FOMC_HTML_YEAR_FMT = "https://www.federalreserve.gov/monetarypolicy/fomccalendars{year}.htm"
FOMC_OVERRIDES = {
    2026: [
        "2026-01-27",
        "2026-03-17",
        "2026-04-28",
        "2026-06-16",
        "2026-07-28",
        "2026-09-15",
        "2026-10-27",
        "2026-12-08",
    ]
}
FRED_BASE = "https://api.stlouisfed.org/fred"
CPI_SERIES_ID = "CPIAUCSL"
NFP_SERIES_ID = "PAYEMS"


@dataclass(frozen=True)
class RiskEvent:
    date: date
    name: str
    category: str
    importance: str = "high"
    time_utc: str | None = None


@dataclass(frozen=True)
class RiskDayStatus:
    date: date
    is_risk_day: bool
    events: tuple[RiskEvent, ...]


def _default_events() -> list[RiskEvent]:
    return [
        RiskEvent(date=date(2025, 1, 29), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="19:00"),
        RiskEvent(date=date(2025, 3, 19), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 5, 7), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 6, 18), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 7, 30), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 9, 17), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 11, 5), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 12, 17), name="FOMC rate decision + press conference", category="FOMC", importance="high", time_utc="18:00"),
        RiskEvent(date=date(2025, 1, 14), name="US CPI", category="CPI", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 2, 13), name="US CPI", category="CPI", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 3, 13), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 4, 15), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 5, 14), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 6, 12), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 7, 10), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 8, 14), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 9, 11), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 10, 9), name="US CPI", category="CPI", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 11, 13), name="US CPI", category="CPI", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 12, 11), name="US CPI", category="CPI", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 1, 3), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 2, 7), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 3, 7), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 4, 4), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 5, 2), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 6, 6), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 7, 3), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 8, 8), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 9, 5), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 10, 3), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="12:30"),
        RiskEvent(date=date(2025, 11, 7), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"),
        RiskEvent(date=date(2025, 12, 5), name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"),
    ]


def _fetch_text(url: str, *, headers: dict[str, str] | None = None) -> str:
    request = Request(url, headers=headers or {})
    with urlopen(request) as response:  # nosec B310
        return response.read().decode("utf-8", errors="replace")


def _load_events_from_store(store_path: Path) -> list[RiskEvent] | None:
    try:
        if not store_path.exists():
            return None
        raw_items = json.loads(store_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    events: list[RiskEvent] = []
    for item in raw_items:
        try:
            events.append(
                RiskEvent(
                    date=date.fromisoformat(item["date"]),
                    name=str(item.get("name", "")),
                    category=str(item.get("category", "")),
                    importance=str(item.get("importance", "high")),
                    time_utc=item.get("time_utc"),
                )
            )
        except Exception:
            continue
    return events or None


class _PlainTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data and data.strip():
            self.parts.append(data.strip())

    def text(self) -> str:
        return " ".join(self.parts)


def _parse_fomc_ics(text: str, start_year: int, end_year: int) -> list[RiskEvent]:
    events: list[RiskEvent] = []
    current_summary: str | None = None
    current_date: date | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("SUMMARY:"):
            current_summary = stripped[len("SUMMARY:") :].strip()
        elif stripped.startswith("DTSTART"):
            if ":" not in stripped:
                continue
            _, value = stripped.split(":", 1)
            try:
                parsed = datetime.strptime(value, "%Y%m%dT%H%M%SZ").date() if "T" in value else datetime.strptime(value, "%Y%m%d").date()
            except Exception:
                continue
            current_date = parsed
        elif stripped == "END:VEVENT":
            if current_date is not None and current_summary and start_year <= current_date.year <= end_year:
                events.append(RiskEvent(date=current_date, name=current_summary, category="FOMC", importance="high"))
            current_summary = None
            current_date = None
    return events


def _parse_fomc_html(text: str, start_year: int, end_year: int) -> list[RiskEvent]:
    section_pattern = re.compile(
        r"<div class=\"panel panel-default\">\s*<div class=\"panel-heading\"><h4><a id=\"[^\"]+\">(\d{4}) FOMC Meetings</a></h4></div>(.*?)(?=<div class=\"panel panel-default\">|$)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    row_pattern = re.compile(
        r"<div class=\"(?:fomc-meeting--shaded\s+)?row\s+fomc-meeting\"[^>]*>.*?"
        r"<div class=\"(?:fomc-meeting--shaded\s+)?fomc-meeting__month[^\"]*\">\s*<strong>([A-Za-z]+)</strong>\s*</div>.*?"
        r"<div class=\"fomc-meeting__date[^\"]*\">\s*([^<]+?)\s*</div>",
        flags=re.IGNORECASE | re.DOTALL,
    )

    seen_dates: set[date] = set()
    events: list[RiskEvent] = []
    for section_match in section_pattern.finditer(text):
        year_value = int(section_match.group(1))
        if not (start_year <= year_value <= end_year):
            continue
        section_html = section_match.group(2)
        for row_match in row_pattern.finditer(section_html):
            month_name = row_match.group(1)
            day_token = row_match.group(2).replace("\u2013", "-").replace("&ndash;", "-").replace(" ", "")
            first_day = day_token.split("-")[0]
            try:
                event_date = datetime.strptime(f"{month_name} {first_day} {year_value}", "%B %d %Y").date()
            except ValueError:
                continue
            if event_date in seen_dates:
                continue
            seen_dates.add(event_date)
            events.append(RiskEvent(date=event_date, name="FOMC Meeting", category="FOMC", importance="high"))
    return events


def _fetch_fomc_html_range(start_year: int, end_year: int) -> list[RiskEvent]:
    events: list[RiskEvent] = []
    fetched_any = False
    for year in range(start_year, end_year + 1):
        url = FOMC_HTML_YEAR_FMT.format(year=year)
        try:
            html_text = _fetch_text(url, headers={"User-Agent": "mybot3-risk-events"})
        except Exception:
            continue
        events.extend(_parse_fomc_html(html_text, year, year))
        fetched_any = True

    if fetched_any:
        unique: list[RiskEvent] = []
        seen: set[tuple[date, str]] = set()
        for event in events:
            key = (event.date, event.name)
            if key in seen:
                continue
            seen.add(key)
            unique.append(event)
        return unique

    html_text = _fetch_text(FOMC_HTML_URL, headers={"User-Agent": "mybot3-risk-events"})
    return _parse_fomc_html(html_text, start_year, end_year)


def _apply_fomc_overrides(events: list[RiskEvent], start_year: int, end_year: int) -> list[RiskEvent]:
    existing = {(event.date, event.name) for event in events}
    extra_events: list[RiskEvent] = []
    for year, iso_dates in FOMC_OVERRIDES.items():
        if not (start_year <= year <= end_year):
            continue
        for iso_value in iso_dates:
            event_date = datetime.strptime(iso_value, "%Y-%m-%d").date()
            key = (event_date, "FOMC Meeting")
            if key in existing:
                continue
            extra_events.append(RiskEvent(date=event_date, name="FOMC Meeting", category="FOMC", importance="high"))
    return events + extra_events


def _fred_release_dates(api_key: str, series_id: str, start_year: int, end_year: int) -> list[date]:
    release_params = urlencode({"series_id": series_id, "api_key": api_key, "file_type": "json"})
    release_response = json.loads(_fetch_text(f"{FRED_BASE}/series/release?{release_params}"))
    release_id = None
    if isinstance(release_response.get("release"), dict):
        release_id = release_response["release"].get("id")
    elif release_response.get("releases"):
        release_id = release_response["releases"][0].get("id")
    if not release_id:
        raise RuntimeError(f"No release id for series {series_id}")

    dates_params = urlencode(
        {
            "release_id": release_id,
            "api_key": api_key,
            "file_type": "json",
            "include_release_dates_with_no_data": "true",
        }
    )
    dates_response = json.loads(_fetch_text(f"{FRED_BASE}/release/dates?{dates_params}"))
    release_dates: list[date] = []
    for item in dates_response.get("release_dates", []):
        try:
            release_date = datetime.strptime(item["date"], "%Y-%m-%d").date()
        except Exception:
            continue
        if start_year <= release_date.year <= end_year:
            release_dates.append(release_date)
    return release_dates


def _filter_nfp_to_first_friday(dates: Iterable[date]) -> list[date]:
    unique_dates: list[date] = []
    seen: set[date] = set()
    for current in sorted(dates):
        if current.weekday() != 4 or current.day > 14 or current in seen:
            continue
        seen.add(current)
        unique_dates.append(current)
    return unique_dates


class RiskDayManager:
    def __init__(self, *, store_path: Path = DEFAULT_RISK_EVENTS_PATH, events: Iterable[RiskEvent] | None = None) -> None:
        self.store_path = store_path
        self._events = list(events) if events is not None else (_load_events_from_store(store_path) or _default_events())
        self._rebuild_index()

    def get_risk_status(self, day: date | str, *, categories: Sequence[str] | None = None) -> RiskDayStatus:
        resolved_day = date.fromisoformat(day) if isinstance(day, str) else day
        events = list(self._events_by_date.get(resolved_day, ()))
        if categories:
            allowed = {category.lower() for category in categories}
            events = [event for event in events if event.category.lower() in allowed]
        return RiskDayStatus(date=resolved_day, is_risk_day=bool(events), events=tuple(events))

    def refresh_from_web(
        self,
        *,
        fred_api_key: str,
        start_year: int | None = None,
        end_year: int | None = None,
    ) -> list[RiskEvent]:
        current_year = datetime.now(timezone.utc).year
        start = current_year if start_year is None else start_year
        end = current_year if end_year is None else end_year
        events: list[RiskEvent] = []

        try:
            fomc_events = _fetch_fomc_html_range(start, end)
        except Exception:
            ics_text = _fetch_text(FOMC_ICS_URL, headers={"User-Agent": "mybot3-risk-events"})
            fomc_events = _parse_fomc_ics(ics_text, start, end)

        events.extend(_apply_fomc_overrides(fomc_events, start, end))

        for release_date in _fred_release_dates(fred_api_key, CPI_SERIES_ID, start, end):
            events.append(RiskEvent(date=release_date, name="US CPI", category="CPI", importance="high", time_utc="13:30"))

        nfp_dates = _filter_nfp_to_first_friday(_fred_release_dates(fred_api_key, NFP_SERIES_ID, start, end))
        for release_date in nfp_dates:
            events.append(RiskEvent(date=release_date, name="US Non-Farm Payrolls", category="NFP", importance="high", time_utc="13:30"))

        deduplicated = self._deduplicate_events(events)
        self._write_store(deduplicated)
        self._events = deduplicated
        self._rebuild_index()
        return list(self._events)

    def _rebuild_index(self) -> None:
        self._events_by_date: dict[date, list[RiskEvent]] = {}
        for event in self._deduplicate_events(self._events):
            self._events_by_date.setdefault(event.date, []).append(event)
        self._events = [event for events in self._events_by_date.values() for event in events]

    @staticmethod
    def _deduplicate_events(events: Iterable[RiskEvent]) -> list[RiskEvent]:
        unique: list[RiskEvent] = []
        seen: set[tuple[date, str, str, str | None]] = set()
        for event in sorted(events, key=lambda item: (item.date, item.category, item.name, item.time_utc or "")):
            key = (event.date, event.name, event.category, event.time_utc)
            if key in seen:
                continue
            seen.add(key)
            unique.append(event)
        return unique

    def _write_store(self, events: Iterable[RiskEvent]) -> None:
        payload = [
            {
                "date": event.date.isoformat(),
                "name": event.name,
                "category": event.category,
                "importance": event.importance,
                "time_utc": event.time_utc,
            }
            for event in events
        ]
        self.store_path.parent.mkdir(parents=True, exist_ok=True)
        self.store_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
