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


DEFAULT_SOURCE_INDEXES = (
    "https://www.reuters.com/arc/outboundfeeds/news-sitemap-index/?outputType=xml",
    "https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml",
)

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
        "--source-index",
        action="append",
        dest="source_indexes",
        help="Reuters sitemap index URL. Repeat to add sources. Defaults to Reuters XML feeds from robots.txt.",
    )
    parser.add_argument("--max-sitemaps", type=int, default=0, help="Limit fetched child sitemaps per source; 0 means no limit")
    parser.add_argument("--max-urls", type=int, default=0, help="Limit processed article URLs; 0 means no limit")
    parser.add_argument("--sleep", type=float, default=0.05, help="Seconds to sleep between sitemap requests")
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
    published_date = published_at.date() if published_at else date_from_url(url)
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
    source_indexes = args.source_indexes or list(DEFAULT_SOURCE_INDEXES)

    calendar_path = Path(args.calendar)
    calendar = json.loads(calendar_path.read_text(encoding="utf-8"))
    watch_index = build_watch_index(calendar)

    deduped: dict[str, dict] = {}
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
        "source_indexes": source_indexes,
        "method": "Reuters XML sitemap metadata matched to active watch-calendar days by strand and threat terms.",
        "items": items,
        "counts": {
            "items": len(items),
            "dates": len(by_date),
            "by_confidence": dict(sorted(by_confidence.items())),
            "by_date": dict(sorted(by_date.items())),
        },
    }

    write_outputs(payload, Path(args.out), Path(args.web_out))
    log(f"wrote {len(items)} correlated Reuters items across {len(by_date)} dates")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
