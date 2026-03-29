#!/usr/bin/env python3
from __future__ import annotations

import gzip
import hashlib
import io
import os
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import yaml


TIME_RE = re.compile(r"^(\d{14})\s*([+-]\d{4})?$")


@dataclass
class SourceConfig:
    name: str
    url: str
    shift_hours: int
    output: str


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(code)


def ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def fetch_gz(url: str, timeout: int = 60) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "timeshiftepg/1.0 (+GitHub Actions)"
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
    if not data:
        die(f"Downloaded empty response from {url}")

    # We expect .gz sources. Sanity check: gzip magic header 1f 8b
    if len(data) < 2 or data[0:2] != b"\x1f\x8b":
        die(f"Source does not look like gzip (.gz): {url}")

    return data


def gunzip_to_text(gz_bytes: bytes) -> str:
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb") as gf:
            raw = gf.read()
    except OSError as e:
        die(f"Failed to decompress gzip: {e}")

    # XMLTV is usually UTF-8, but some feeds may include an encoding declaration.
    # ElementTree can accept bytes directly, but we want a text string for optional checks.
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("utf-8", errors="replace")


def parse_xml(xml_text: str) -> ET.ElementTree:
    # Quick guard against HTML error pages
    sniff = xml_text.lstrip()[:200].lower()
    if sniff.startswith("<!doctype html") or sniff.startswith("<html"):
        die("Downloaded content looks like HTML, not XMLTV XML")

    try:
        return ET.ElementTree(ET.fromstring(xml_text))
    except ET.ParseError as e:
        die(f"XML parse error: {e}")


def parse_xmltv_time(value: str) -> tuple[datetime, str]:
    """
    Returns (utc_datetime, original_offset_string_or_+0000).

    XMLTV time is typically: YYYYMMDDhhmmss +ZZZZ
    We interpret it as an absolute moment in time.
    """
    m = TIME_RE.match(value.strip())
    if not m:
        die(f"Unrecognized time format: {value!r}")

    dt_part = m.group(1)
    off_part = m.group(2) or "+0000"

    dt_naive = datetime.strptime(dt_part, "%Y%m%d%H%M%S")

    sign = 1 if off_part[0] == "+" else -1
    off_hours = int(off_part[1:3])
    off_mins = int(off_part[3:5])
    offset = timedelta(hours=off_hours, minutes=off_mins) * sign

    aware = dt_naive.replace(tzinfo=timezone(offset))
    utc_dt = aware.astimezone(timezone.utc)

    return utc_dt, off_part


def format_xmltv_time(utc_dt: datetime) -> str:
    # We always output UTC and declare +0000
    if utc_dt.tzinfo is None:
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    utc_dt = utc_dt.astimezone(timezone.utc)
    return utc_dt.strftime("%Y%m%d%H%M%S") + " +0000"


def shift_programme_times(tree: ET.ElementTree, shift_hours: int) -> int:
    root = tree.getroot()
    delta = timedelta(hours=shift_hours)
    changed = 0

    for prog in root.findall("programme"):
        for attr in ("start", "stop"):
            v = prog.get(attr)
            if not v:
                continue
            utc_dt, _ = parse_xmltv_time(v)
            shifted = utc_dt + delta
            prog.set(attr, format_xmltv_time(shifted))
            changed += 1

    return changed


def write_gz(tree: ET.ElementTree, out_path: str) -> bytes:
    # Keep XML declaration and UTF-8 output
    xml_bytes = ET.tostring(tree.getroot(), encoding="utf-8", xml_declaration=True)

    buf = io.BytesIO()
    with gzip.GzipFile(filename=os.path.basename(out_path), fileobj=buf, mode="wb", compresslevel=9) as gf:
        gf.write(xml_bytes)
    gz_bytes = buf.getvalue()

    ensure_parent_dir(out_path)
    with open(out_path, "wb") as f:
        f.write(gz_bytes)

    return gz_bytes


def load_config(path: str) -> list[SourceConfig]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    if not isinstance(cfg, dict) or "sources" not in cfg:
        die("sources.yaml must contain a top-level 'sources' list")

    sources = cfg["sources"]
    if not isinstance(sources, list) or not sources:
        die("'sources' must be a non-empty list")

    parsed: list[SourceConfig] = []
    for i, s in enumerate(sources):
        if not isinstance(s, dict):
            die(f"Source #{i} is not a mapping/object")

        name = str(s.get("name", "")).strip()
        url = str(s.get("url", "")).strip()
        output = str(s.get("output", "")).strip()
        shift_hours = s.get("shift_hours", None)

        if not name:
            die(f"Source #{i} missing 'name'")
        if not url:
            die(f"Source '{name}' missing 'url'")
        if not url.endswith(".gz"):
            die(f"Source '{name}' url must end with .gz")
        if shift_hours is None or not isinstance(shift_hours, int):
            die(f"Source '{name}' shift_hours must be an integer")
        if not output:
            die(f"Source '{name}' missing 'output'")
        if not output.endswith(".gz"):
            die(f"Source '{name}' output must end with .gz")

        parsed.append(SourceConfig(name=name, url=url, shift_hours=shift_hours, output=output))

    return parsed


def main() -> int:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "sources.yaml"
    sources = load_config(config_path)

    print(f"Loaded {len(sources)} source(s) from {config_path}")

    for src in sources:
        print(f"\nProcessing: {src.name}")
        print(f"  Fetch:   {src.url}")
        print(f"  Shift:   {src.shift_hours} hour(s)")
        print(f"  Output:  {src.output}")

        try:
            gz_in = fetch_gz(src.url)
            in_hash = sha256_bytes(gz_in)
            xml_text = gunzip_to_text(gz_in)
            tree = parse_xml(xml_text)

            changed = shift_programme_times(tree, src.shift_hours)
            gz_out = write_gz(tree, src.output)
            out_hash = sha256_bytes(gz_out)

            print(f"  Updated attributes: {changed}")
            print(f"  Input sha256:  {in_hash}")
            print(f"  Output sha256: {out_hash}")
            print(f"  Output size:   {len(gz_out)} bytes")

        except SystemExit:
            print(f"  Skipping source due to fetch/decode/parse error: {src.name}")
            continue
        except Exception as e:
            print(f"  Unexpected error for source {src.name}: {e}", file=sys.stderr)
            continue

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
