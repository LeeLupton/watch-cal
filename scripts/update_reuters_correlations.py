#!/usr/bin/env python3
"""Generate Reuters correlation data for the watch calendar.

The script uses only Python's standard library. It reads the static watch
calendar, fetches Reuters XML sitemap feeds exposed from robots.txt, and writes
both a canonical JSON file and a browser wrapper consumed by the static app.
"""

from __future__ import annotations

import argparse
import datetime as dt
import html
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path


ROLLING_SOURCE_INDEXES = (
    "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml",
    "https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml",
)
DEFAULT_START_DATE = "2025-05-01"
DEFAULT_DAILY_SITEMAP_TEMPLATE = "https://www.reuters.com/arc/outboundfeeds/sitemap3/{date}/?outputType=xml"

NS = {
    "sm": "http://www.sitemaps.org/schemas/sitemap/0.9",
    "news": "http://www.google.com/schemas/sitemap-news/0.9",
    "image": "http://www.google.com/schemas/sitemap-image/1.1",
}

LEVELS = ["NONE", "BASELINE", "MODERATE", "ELEVATED", "CRITICAL"]
LEVEL_RANK = {level: rank for rank, level in enumerate(LEVELS)}
LEAD_DAYS = {"NONE": 0, "BASELINE": 3, "MODERATE": 7, "ELEVATED": 14, "CRITICAL": 30}
STRANDS = ("SJ", "SI", "PAL", "MB", "PI")

LOCALIZED_PREFIXES = {
    "ar",
    "de",
    "es",
    "fr",
    "it",
    "ja",
    "pt",
    "ru",
    "zh",
}

GENERIC_EXCLUDED_SECTIONS = {
    "business",
    "legal",
    "lifestyle",
    "markets",
    "podcasts",
    "science",
    "sports",
    "sustainability",
    "technology",
}

HIGH_THREAT_TERMS = (
    "attack",
    "attacks",
    "bomb",
    "bombing",
    "ceasefire",
    "drone",
    "hostage",
    "insurgent",
    "militant",
    "missile",
    "rocket",
    "stabbed",
    "stabbing",
    "strike",
    "strikes",
    "terror",
    "terrorism",
    "war",
)

MEDIUM_CONFLICT_TERMS = (
    "clash",
    "conflict",
    "crisis",
    "deadlock",
    "escalation",
    "fighting",
    "military",
    "peacekeeping",
    "proxy",
    "sanction",
    "security",
    "tension",
    "troops",
    "unrest",
    "violence",
)

STRAND_HIGH_TERMS = {
    "SJ": (
        "al qaeda",
        "al-qaeda",
        "aqap",
        "boko haram",
        "caliphate",
        "fall of mosul",
        "isis",
        "islamic state",
        "islamic state-linked",
        "jihad",
        "jihadist",
        "mosul",
        "shabaab",
        "taliban",
    ),
    "SI": (
        "axis of resistance",
        "hezbollah",
        "hormuz",
        "houthi",
        "huthis",
        "iran",
        "iranian",
        "irgc",
        "khamenei",
        "popular mobilization",
        "revolutionary guard",
        "shia",
        "shiite",
        "shi'ite",
        "soleimani",
        "tehran",
    ),
    "PAL": (
        "hamas",
        "islamic jihad",
        "jerusalem",
        "oct 7",
        "october 7",
        "palestinian",
        "palestinians",
        "west bank",
    ),
    "MB": (
        "banna",
        "morsi",
        "muslim brotherhood",
        "qutb",
        "rabaa",
        "sadat",
    ),
    "PI": (
        "ashura",
        "caliphate",
        "eid",
        "islamic",
        "islamist",
        "mosque",
        "muharram",
        "muslim",
        "quran",
        "ramadan",
        "sectarian",
    ),
}

STRAND_MEDIUM_TERMS = {
    "SJ": (
        "afghanistan",
        "iraq",
        "mali",
        "nigeria",
        "pakistan",
        "sahel",
        "somalia",
        "syria",
    ),
    "SI": (
        "iraq",
        "lebanon",
        "red sea",
        "syria",
        "yemen",
    ),
    "PAL": (
        "gaza",
        "israel",
        "israeli",
        "rafah",
    ),
    "MB": (
        "egypt",
        "egyptian",
        "turkey",
        "turkish",
    ),
    "PI": (
        "religious",
        "sectarian",
    ),
}

EVENT_KEY_TERMS = {
    "soleimani": ("soleimani", "al-muhandis", "muhandis"),
    "islamic revolution": ("islamic revolution", "khomeini", "iran revolution"),
    "banna": ("banna", "muslim brotherhood"),
    "soviet withdrawal": ("afghanistan", "soviet"),
    "ramadan": ("ramadan",),
    "badr": ("badr", "ramadan"),
    "caliphate": ("caliphate",),
    "madrid": ("madrid",),
    "eid": ("eid",),
    "nakba": ("nakba", "palestinian"),
    "hezbollah": ("hezbollah", "lebanon"),
    "ashura": ("ashura", "shia", "shiite", "shi'ite"),
    "mosul": ("mosul", "islamic state", "isis"),
    "morsi": ("morsi",),
    "london 7/7": ("london", "terror"),
    "embassy bombings": ("embassy", "bombing"),
    "rabaa": ("rabaa",),
    "qutb": ("qutb",),
    "9/11": ("9/11", "september 11"),
    "intifada": ("intifada", "palestinian"),
    "awlaki": ("awlaki", "aqap"),
    "sadat": ("sadat",),
    "oct 7": ("oct 7", "october 7", "hamas"),
    "uss cole": ("uss cole", "bali"),
    "beirut": ("beirut", "hezbollah"),
    "baghdadi": ("baghdadi", "isis"),
    "embassy seizure": ("embassy", "tehran", "iran"),
    "paris": ("paris", "terror"),
    "grand mosque": ("grand mosque", "mecca"),
}


def log(message: str) -> None:
    print(message, file=sys.stderr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--calendar", default="calendar_2026_2027.json", help="Calendar JSON input")
    parser.add_argument("--out", default="reuters_correlations.json", help="Canonical JSON output")
    parser.add_argument("--web-out", default="web/reuters-data.js", help="Browser JS output")
    parser.add_argument("--as-of", default=dt.date.today().isoformat(), help="Exclusive past-date cutoff, YYYY-MM-DD")
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive historical coverage start date for Reuters day-sitemap backfill, YYYY-MM-DD",
    )
    parser.add_argument(
        "--source-index",
        action="append",
        dest="source_indexes",
        help="Optional Reuters rolling sitemap index URL. Repeat to add sources.",
    )
    parser.add_argument(
        "--include-rolling-indexes",
        action="store_true",
        help="Also fetch Reuters rolling XML sitemap indexes from robots.txt.",
    )
    parser.add_argument(
        "--daily-sitemap-template",
        default=DEFAULT_DAILY_SITEMAP_TEMPLATE,
        help="Reuters daily sitemap URL template. Use {date} for YYYY-MM-DD.",
    )
    parser.add_argument("--no-daily-sitemaps", action="store_true", help="Skip Reuters day-sitemap backfill")
    parser.add_argument("--no-source-indexes", action="store_true", help="Skip Reuters rolling sitemap indexes")
    parser.add_argument("--max-days", type=int, default=0, help="Limit fetched active day sitemaps; 0 means no limit")
    parser.add_argument("--max-sitemaps", type=int, default=0, help="Limit fetched child sitemaps per source; 0 means no limit")
    parser.add_argument("--max-urls", type=int, default=0, help="Limit processed article URLs; 0 means no limit")
    parser.add_argument("--sleep", type=float, default=0.05, help="Seconds to sleep between sitemap requests")
    parser.add_argument("--timeout", type=int, default=12, help="HTTP timeout in seconds for Reuters sitemap requests")
    return parser.parse_args()


def parse_date(value: str) -> dt.date:
    clean = value.strip().removeprefix("~")
    y, m, d = (int(part) for part in clean.split("-"))
    return dt.date(y, m, d)


def parse_date_range(value: str) -> tuple[dt.date, dt.date]:
    if "/" in value:
        start, end = value.split("/", 1)
        return parse_date(start), parse_date(end)
    date = parse_date(value)
    return date, date


def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur += dt.timedelta(days=1)


def anniversary_suffix(value: int) -> str:
    if 10 <= value % 100 <= 20:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")


def retarget_anniversary_name(name: str, year_origin: int | None, target_year: int) -> str:
    if not year_origin:
        return name
    anniversary = target_year - int(year_origin)
    if anniversary <= 0:
        return name
    label = f"{anniversary}{anniversary_suffix(anniversary)} anniv."
    return re.sub(r"\d+(?:st|nd|rd|th) anniv\.", label, name, count=1)


def projection_stem(name: str) -> str:
    stem = re.sub(r"\s*\(\d+(?:st|nd|rd|th) anniv\.\)", "", name, flags=re.IGNORECASE)
    stem = re.sub(r"\s+", " ", stem.lower())
    return stem.strip()


def retarget_date_value(value: str, target_year: int) -> str | None:
    if "~" in value:
        return None
    parts = value.split("/")
    retargeted: list[str] = []
    for part in parts:
        try:
            source_date = parse_date(part)
            retargeted.append(dt.date(target_year, source_date.month, source_date.day).isoformat())
        except ValueError:
            return None
    return "/".join(retargeted)


def active_span_for_event(event: dict) -> tuple[dt.date, dt.date]:
    start, end = parse_date_range(event["date"])
    level = event.get("vigilance_overall", "NONE")
    lead = int(event.get("lead_time_days") or LEAD_DAYS.get(level, 0))
    return start - dt.timedelta(days=lead), end


def ranges_overlap(left_start: dt.date, left_end: dt.date, right_start: dt.date, right_end: dt.date) -> bool:
    return left_start <= right_end and right_start <= left_end


def event_projection_key(event: dict) -> tuple[int, int, str]:
    start, _ = parse_date_range(event["date"])
    return start.month, start.day, projection_stem(event.get("name", ""))


def window_projection_key(window: dict) -> tuple[int, int, int, int, str]:
    start = parse_date(window["start"])
    end = parse_date(window["end"])
    label = re.sub(r"\b20\d{2}\b", "", window.get("label", ""))
    label = re.sub(r"\s+", " ", label.lower()).strip()
    return start.month, start.day, end.month, end.day, label


def project_calendar_history(calendar: dict, start_date: dt.date, end_date: dt.date) -> tuple[dict, dict]:
    """Add recurring Gregorian watch context before the shipped 2026/2027 calendar.

    The source calendar intentionally focuses on 2026-2027, but Reuters backfill
    needs the same recurring Gregorian anniversaries when matching older dates.
    Hijri/approximate dates are left untouched because projecting them by the
    Gregorian year would be misleading.
    """

    projected = dict(calendar)
    projected_events = [dict(event) for event in calendar.get("events", [])]
    projected_windows = [dict(window) for window in calendar.get("compound_windows", [])]

    target_years = range(start_date.year, end_date.year + 1)
    coverage_start = start_date
    coverage_end = end_date

    base_events: dict[tuple[int, int, str], dict] = {}
    existing_events: set[tuple[str, str]] = set()
    for event in calendar.get("events", []):
        if event.get("type") != "Gregorian":
            continue
        if "~" in event.get("date", "") or "/" in event.get("date", ""):
            continue
        try:
            start, _ = parse_date_range(event["date"])
        except ValueError:
            continue

        key = event_projection_key(event)
        existing_events.add((event["date"], key[2]))
        current = base_events.get(key)
        if not current:
            base_events[key] = event
            continue
        current_start, _ = parse_date_range(current["date"])
        if start < current_start:
            base_events[key] = event

    added_events = 0
    for key, event in base_events.items():
        for year in target_years:
            retargeted_date = retarget_date_value(event["date"], year)
            if not retargeted_date or (retargeted_date, key[2]) in existing_events:
                continue

            clone = dict(event)
            clone["date"] = retargeted_date
            clone["name"] = retarget_anniversary_name(
                event.get("name", ""),
                event.get("year_origin"),
                year,
            )
            clone["historical_projection"] = True
            clone["projected_from_date"] = event["date"]

            active_start, active_end = active_span_for_event(clone)
            if not ranges_overlap(active_start, active_end, coverage_start, coverage_end):
                continue

            projected_events.append(clone)
            existing_events.add((retargeted_date, key[2]))
            added_events += 1

    base_windows: dict[tuple[int, int, int, int, str], dict] = {}
    existing_windows: set[tuple[str, str, str]] = set()
    for window in calendar.get("compound_windows", []):
        try:
            start = parse_date(window["start"])
            parse_date(window["end"])
        except ValueError:
            continue
        if any("~" in str(window.get(part, "")) for part in ("start", "end")):
            continue
        if any(retarget_date_value(anchor, start.year) is None for anchor in window.get("anchor_dates", [])):
            continue

        key = window_projection_key(window)
        existing_windows.add((window["start"], window["end"], key[4]))
        current = base_windows.get(key)
        if not current:
            base_windows[key] = window
            continue
        current_start = parse_date(current["start"])
        if start < current_start:
            base_windows[key] = window

    added_windows = 0
    for key, window in base_windows.items():
        for year in target_years:
            retargeted_start = retarget_date_value(window["start"], year)
            retargeted_end = retarget_date_value(window["end"], year)
            if not retargeted_start or not retargeted_end:
                continue
            if (retargeted_start, retargeted_end, key[4]) in existing_windows:
                continue

            start = parse_date(retargeted_start)
            end = parse_date(retargeted_end)
            if not ranges_overlap(start, end, coverage_start, coverage_end):
                continue

            clone = dict(window)
            clone["start"] = retargeted_start
            clone["end"] = retargeted_end
            clone["year"] = year
            clone["id"] = re.sub(r"\b20\d{2}\b", str(year), window.get("id", ""))
            if clone["id"] == window.get("id", ""):
                clone["id"] = f"{window.get('id', 'CW')}-{year}"
            clone["anchor_dates"] = [
                retargeted
                for anchor in window.get("anchor_dates", [])
                if (retargeted := retarget_date_value(anchor, year))
            ]
            clone["historical_projection"] = True
            clone["projected_from_id"] = window.get("id")

            projected_windows.append(clone)
            existing_windows.add((retargeted_start, retargeted_end, key[4]))
            added_windows += 1

    projected["events"] = projected_events
    projected["compound_windows"] = projected_windows
    return projected, {"events": added_events, "compound_windows": added_windows}


def ensure_entry(index: dict[str, dict], key: str) -> dict:
    if key not in index:
        index[key] = {
            "level": "NONE",
            "strand_levels": {},
            "anchors": [],
            "postures": [],
            "windows": [],
            "window_strands": set(),
        }
    return index[key]


def raise_level(entry: dict, level: str) -> None:
    if LEVEL_RANK.get(level, 0) > LEVEL_RANK.get(entry["level"], 0):
        entry["level"] = level


def raise_strand(entry: dict, strand: str, level: str) -> None:
    current = entry["strand_levels"].get(strand, "NONE")
    if LEVEL_RANK.get(level, 0) > LEVEL_RANK.get(current, 0):
        entry["strand_levels"][strand] = level


def infer_strands_from_text(text: str) -> set[str]:
    haystack = normalize_text(text)
    strands: set[str] = set()
    for strand in STRANDS:
        if any(term in haystack for term in STRAND_HIGH_TERMS[strand] + STRAND_MEDIUM_TERMS[strand]):
            strands.add(strand)
    return strands


def build_watch_index(calendar: dict) -> dict[str, dict]:
    index: dict[str, dict] = {}
    events_by_date = defaultdict(list)

    for event in calendar.get("events", []):
        start, end = parse_date_range(event["date"])
        level = event.get("vigilance_overall", "NONE")
        lead = int(event.get("lead_time_days") or LEAD_DAYS.get(level, 0))
        strands = event.get("vigilance_by_strand") or {}
        events_by_date[event["date"]].append(event)

        for cur in daterange(start, end):
            entry = ensure_entry(index, cur.isoformat())
            raise_level(entry, level)
            entry["anchors"].append(event)
            for strand, strand_level in strands.items():
                raise_strand(entry, strand, strand_level)

        for cur in daterange(start - dt.timedelta(days=lead), start - dt.timedelta(days=1)):
            entry = ensure_entry(index, cur.isoformat())
            raise_level(entry, level)
            entry["postures"].append({"event": event, "tMinus": (start - cur).days})
            for strand, strand_level in strands.items():
                raise_strand(entry, strand, strand_level)

    for window in calendar.get("compound_windows", []):
        w_start = parse_date(window["start"])
        w_end = parse_date(window["end"])
        text = " ".join(str(window.get(part, "")) for part in ("id", "label", "rationale"))
        window_strands = infer_strands_from_text(text)
        for anchor_date in window.get("anchor_dates", []):
            for event in events_by_date.get(anchor_date, []):
                window_strands.update((event.get("vigilance_by_strand") or {}).keys())

        for cur in daterange(w_start, w_end):
            entry = ensure_entry(index, cur.isoformat())
            raise_level(entry, window.get("level", "NONE"))
            entry["windows"].append(window)
            entry["window_strands"].update(window_strands)

    return index


def normalize_text(value: str) -> str:
    value = html.unescape(value or "").lower()
    value = re.sub(r"[\u2018\u2019]", "'", value)
    value = re.sub(r"[\u201c\u201d]", '"', value)
    value = re.sub(r"[^a-z0-9/' -]+", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def compact_reason(prefix: str, term: str) -> str:
    return f"{prefix}: {term}"


def fetch_xml(url: str, timeout: int = 30) -> ET.Element:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/xml,text/xml,*/*;q=0.8",
            "User-Agent": "watch-cal-reuters-correlation/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read()
    return ET.fromstring(body)


def child_text(node: ET.Element, path: str) -> str:
    child = node.find(path, NS)
    return child.text.strip() if child is not None and child.text else ""


def collect_sitemap_urls(source_url: str) -> list[str]:
    root = fetch_xml(source_url)
    if root.tag.endswith("sitemapindex"):
        return [child_text(node, "sm:loc") for node in root.findall("sm:sitemap", NS) if child_text(node, "sm:loc")]
    if root.tag.endswith("urlset"):
        return [source_url]
    return []


def section_from_url(url: str) -> str:
    path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
    if not path_parts:
        return "reuters"
    if path_parts[0] in LOCALIZED_PREFIXES:
        return path_parts[1] if len(path_parts) > 1 else path_parts[0]
    return path_parts[0]


def is_localized_url(url: str) -> bool:
    path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
    return bool(path_parts and path_parts[0] in LOCALIZED_PREFIXES)


def canonical_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


def title_from_url(url: str) -> str:
    path_parts = [part for part in urllib.parse.urlparse(url).path.split("/") if part]
    if not path_parts:
        return "Reuters story"
    slug = path_parts[-1]
    slug = re.sub(r"-\d{4}-\d{2}-\d{2}$", "", slug)
    slug = re.sub(r"-flm$", "", slug)
    words = [word.upper() if word in {"us", "uk", "eu", "un", "ufo"} else word for word in slug.split("-")]
    title = " ".join(words).strip()
    return (title[:1].upper() + title[1:]) if title else "Reuters story"


def date_from_url(url: str) -> dt.date | None:
    match = re.search(r"-(\d{4}-\d{2}-\d{2})/?$", urllib.parse.urlparse(url).path)
    if not match:
        return None
    try:
        return dt.date.fromisoformat(match.group(1))
    except ValueError:
        return None


def parse_datetime(value: str) -> dt.datetime | None:
    if not value:
        return None
    clean = value.strip().replace("Z", "+00:00")
    try:
        return dt.datetime.fromisoformat(clean)
    except ValueError:
        return None


def parse_article_node(node: ET.Element) -> dict | None:
    url = child_text(node, "sm:loc")
    if not url or "reuters.com" not in urllib.parse.urlparse(url).netloc:
        return None
    if is_localized_url(url):
        return None
    title = child_text(node, "news:news/news:title") or title_from_url(url)
    publication_value = child_text(node, "news:news/news:publication_date") or child_text(node, "sm:lastmod")
    published_at = parse_datetime(publication_value)
    published_date = date_from_url(url) or (published_at.date() if published_at else None)
    if not published_date:
        return None

    image_url = child_text(node, "image:image/image:loc")
    if image_url:
        image_url = html.unescape(image_url)

    return {
        "date": published_date.isoformat(),
        "title": html.unescape(title).strip(),
        "url": canonical_url(html.unescape(url)),
        "publication_date": published_at.isoformat().replace("+00:00", "Z") if published_at else published_date.isoformat(),
        "section": section_from_url(url),
        "image_url": image_url,
    }


def iter_articles(source_indexes: list[str], max_sitemaps: int, max_urls: int, sleep: float):
    processed_urls = 0
    for source_url in source_indexes:
        try:
            sitemap_urls = collect_sitemap_urls(source_url)
        except (urllib.error.URLError, ET.ParseError) as exc:
            log(f"warning: could not read source index {source_url}: {exc}")
            continue

        if max_sitemaps:
            sitemap_urls = sitemap_urls[:max_sitemaps]
        log(f"source: {source_url} ({len(sitemap_urls)} sitemap feeds)")

        for idx, sitemap_url in enumerate(sitemap_urls, 1):
            if max_urls and processed_urls >= max_urls:
                return
            try:
                root = fetch_xml(sitemap_url)
            except (urllib.error.URLError, ET.ParseError) as exc:
                log(f"warning: could not read sitemap {sitemap_url}: {exc}")
                continue

            nodes = root.findall("sm:url", NS)
            log(f"  sitemap {idx}/{len(sitemap_urls)}: {len(nodes)} urls")
            for node in nodes:
                if max_urls and processed_urls >= max_urls:
                    return
                article = parse_article_node(node)
                processed_urls += 1
                if article:
                    yield article
            if sleep:
                time.sleep(sleep)


def active_watch_dates(watch_index: dict[str, dict], start_date: dt.date, as_of: dt.date) -> list[dt.date]:
    dates: list[dt.date] = []
    for key, entry in watch_index.items():
        day = dt.date.fromisoformat(key)
        if start_date <= day < as_of and LEVEL_RANK.get(entry.get("level", "NONE"), 0) > 0:
            dates.append(day)
    return sorted(set(dates))


def iter_daily_sitemap_articles(
    dates: list[dt.date],
    template: str,
    max_days: int,
    max_urls: int,
    sleep: float,
    timeout: int,
    failures: list[dict],
):
    processed_urls = 0
    for idx, day in enumerate(dates, 1):
        if max_days and idx > max_days:
            return
        if max_urls and processed_urls >= max_urls:
            return

        sitemap_url = template.format(date=day.isoformat())
        try:
            root = fetch_xml(sitemap_url, timeout=timeout)
        except urllib.error.HTTPError as exc:
            failures.append({"date": day.isoformat(), "url": sitemap_url, "error": f"HTTP {exc.code}"})
            log(f"warning: could not read day sitemap {day}: HTTP {exc.code}")
            continue
        except (TimeoutError, urllib.error.URLError, ET.ParseError, ValueError) as exc:
            failures.append({"date": day.isoformat(), "url": sitemap_url, "error": str(exc)})
            log(f"warning: could not read day sitemap {day}: {exc}")
            continue

        nodes = root.findall("sm:url", NS)
        log(f"  day sitemap {idx}/{len(dates)} {day}: {len(nodes)} urls")
        for node in nodes:
            if max_urls and processed_urls >= max_urls:
                return
            article = parse_article_node(node)
            processed_urls += 1
            if article:
                yield article
        if sleep:
            time.sleep(sleep)


def find_terms(haystack: str, terms: tuple[str, ...]) -> list[str]:
    matches: list[str] = []
    for term in terms:
        normalized = normalize_text(term)
        if not normalized:
            continue
        parts = [re.escape(part) for part in re.split(r"[\s-]+", normalized) if part]
        if not parts:
            continue
        pattern = r"(?<![a-z0-9])" + r"[\s-]+".join(parts) + r"(?![a-z0-9])"
        if re.search(pattern, haystack):
            matches.append(term)
    return matches


def event_terms_for(entry: dict) -> list[str]:
    terms: list[str] = []
    events = [item for item in entry.get("anchors", [])]
    events.extend(posture["event"] for posture in entry.get("postures", []))
    for event in events:
        event_name = normalize_text(event.get("name", ""))
        for key, mapped_terms in EVENT_KEY_TERMS.items():
            if key in event_name:
                terms.extend(mapped_terms)
    for window in entry.get("windows", []):
        terms.extend(infer_terms_from_window(window))
    return sorted(set(terms))


def infer_terms_from_window(window: dict) -> list[str]:
    text = normalize_text(" ".join(str(window.get(part, "")) for part in ("label", "rationale")))
    terms = []
    for key, mapped_terms in EVENT_KEY_TERMS.items():
        if key in text:
            terms.extend(mapped_terms)
    return terms


def active_strands(entry: dict) -> set[str]:
    strands = {strand for strand, level in entry.get("strand_levels", {}).items() if level != "NONE"}
    strands.update(entry.get("window_strands", set()))
    return strands


def event_names_for_strands(entry: dict, strands: set[str]) -> list[str]:
    names: list[str] = []
    for event in entry.get("anchors", []):
        event_strands = set((event.get("vigilance_by_strand") or {}).keys())
        if not strands or event_strands.intersection(strands):
            names.append(event.get("name", "Watch event"))
    for posture in entry.get("postures", []):
        event = posture["event"]
        event_strands = set((event.get("vigilance_by_strand") or {}).keys())
        if not strands or event_strands.intersection(strands):
            names.append(event.get("name", "Watch event"))
    for window in entry.get("windows", []):
        names.append(window.get("label") or window.get("id") or "Compound window")
    return sorted(dict.fromkeys(name for name in names if name))[:8]


def classify_article(article: dict, entry: dict, as_of: dt.date) -> dict | None:
    article_date = dt.date.fromisoformat(article["date"])
    if article_date >= as_of:
        return None
    if LEVEL_RANK.get(entry.get("level", "NONE"), 0) <= 0:
        return None

    haystack = normalize_text(" ".join((article.get("title", ""), article.get("url", ""), article.get("section", ""))))
    section = article.get("section", "")
    active = active_strands(entry)
    if not active:
        active = infer_strands_from_text(" ".join(window.get("label", "") for window in entry.get("windows", [])))
    if not active:
        return None

    threat_matches = find_terms(haystack, HIGH_THREAT_TERMS)
    conflict_matches = find_terms(haystack, MEDIUM_CONFLICT_TERMS)
    event_matches = find_terms(haystack, tuple(event_terms_for(entry)))

    matched_strands: set[str] = set()
    reasons: list[str] = []
    high_signal = False
    medium_signal = False

    if event_matches:
        high_signal = True
        reasons.extend(compact_reason("watch term", term) for term in event_matches[:3])

    for strand in sorted(active):
        high_terms = find_terms(haystack, STRAND_HIGH_TERMS[strand])
        medium_terms = find_terms(haystack, STRAND_MEDIUM_TERMS[strand])

        if high_terms and (threat_matches or event_matches):
            matched_strands.add(strand)
            high_signal = True
            reasons.append(compact_reason(f"{strand} term", high_terms[0]))
        elif high_terms:
            matched_strands.add(strand)
            medium_signal = True
            reasons.append(compact_reason(f"{strand} term", high_terms[0]))
        elif medium_terms and (threat_matches or conflict_matches):
            matched_strands.add(strand)
            medium_signal = True
            reasons.append(compact_reason(f"{strand} context", medium_terms[0]))

    if threat_matches:
        reasons.append(compact_reason("threat term", threat_matches[0]))
    elif conflict_matches:
        reasons.append(compact_reason("conflict term", conflict_matches[0]))

    if not matched_strands and event_matches:
        matched_strands = set(active)

    if not matched_strands:
        return None

    if section in GENERIC_EXCLUDED_SECTIONS and not high_signal:
        return None

    confidence = "HIGH" if high_signal else "MEDIUM" if medium_signal else ""
    if not confidence:
        return None

    if not reasons:
        reasons.append("matched active watch context")

    correlated = dict(article)
    correlated.update(
        {
            "matched_strands": sorted(matched_strands),
            "matched_watch_events": event_names_for_strands(entry, matched_strands),
            "confidence": confidence,
            "match_reasons": sorted(dict.fromkeys(reasons))[:6],
        }
    )
    return correlated


def dedupe_articles(existing: dict[str, dict], article: dict) -> None:
    current = existing.get(article["url"])
    if not current:
        existing[article["url"]] = article
        return
    current_title = current.get("title") or ""
    new_title = article.get("title") or ""
    if current_title == title_from_url(current["url"]) and new_title != title_from_url(article["url"]):
        existing[article["url"]] = article
        return
    if article.get("image_url") and not current.get("image_url"):
        current["image_url"] = article["image_url"]


def write_outputs(payload: dict, out_path: Path, web_out_path: Path) -> None:
    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    web_out_path.parent.mkdir(parents=True, exist_ok=True)
    web_out_path.write_text(
        "window.REUTERS_CORRELATIONS = "
        + json.dumps(payload, indent=2, sort_keys=True)
        + ";\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    as_of = dt.date.fromisoformat(args.as_of)
    start_date = dt.date.fromisoformat(args.start_date)
    if start_date >= as_of:
        log(f"error: --start-date ({start_date}) must be before --as-of ({as_of})")
        return 2
    source_indexes: list[str] = []
    if args.include_rolling_indexes:
        source_indexes.extend(ROLLING_SOURCE_INDEXES)
    if args.source_indexes:
        source_indexes.extend(args.source_indexes)
    if args.no_source_indexes:
        source_indexes = []

    calendar_path = Path(args.calendar)
    calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
    matching_calendar, projection_stats = project_calendar_history(
        calendar,
        start_date,
        as_of - dt.timedelta(days=1),
    )
    watch_index = build_watch_index(matching_calendar)
    daily_dates = active_watch_dates(watch_index, start_date, as_of)

    deduped: dict[str, dict] = {}
    daily_failures: list[dict] = []
    if not args.no_daily_sitemaps:
        log(f"source: Reuters day sitemaps ({len(daily_dates)} active dates from {start_date} to {as_of - dt.timedelta(days=1)})")
        for raw_article in iter_daily_sitemap_articles(
            daily_dates,
            args.daily_sitemap_template,
            args.max_days,
            args.max_urls,
            args.sleep,
            args.timeout,
            daily_failures,
        ):
            entry = watch_index.get(raw_article["date"])
            if not entry:
                continue
            correlated = classify_article(raw_article, entry, as_of)
            if correlated:
                dedupe_articles(deduped, correlated)

    for raw_article in iter_articles(source_indexes, args.max_sitemaps, args.max_urls, args.sleep):
        entry = watch_index.get(raw_article["date"])
        if not entry:
            continue
        correlated = classify_article(raw_article, entry, as_of)
        if correlated:
            dedupe_articles(deduped, correlated)

    items = sorted(
        deduped.values(),
        key=lambda item: (item["date"], item["publication_date"], item["title"]),
        reverse=True,
    )
    by_date = defaultdict(int)
    by_confidence = defaultdict(int)
    for item in items:
        by_date[item["date"]] += 1
        by_confidence[item["confidence"]] += 1

    payload = {
        "schema_version": "1.0",
        "generated": dt.datetime.now(dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "as_of": as_of.isoformat(),
        "start_date": start_date.isoformat(),
        "source_indexes": source_indexes,
        "daily_sitemap_template": None if args.no_daily_sitemaps else args.daily_sitemap_template,
        "method": "Reuters XML sitemap metadata matched to active watch-calendar days by strand and threat terms. Gregorian watch anniversaries are projected backward for historical Reuters backfill; Hijri/approximate dates are not projected.",
        "projection_stats": projection_stats,
        "active_dates_scanned": len(daily_dates) if not args.no_daily_sitemaps else 0,
        "daily_sitemap_failures": daily_failures,
        "items": items,
        "counts": {
            "items": len(items),
            "dates": len(by_date),
            "daily_sitemap_failures": len(daily_failures),
            "by_confidence": dict(sorted(by_confidence.items())),
            "by_date": dict(sorted(by_date.items())),
        },
    }

    write_outputs(payload, Path(args.out), Path(args.web_out))
    log(f"wrote {len(items)} correlated Reuters items across {len(by_date)} dates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
