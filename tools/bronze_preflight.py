"""Aggregate-only, read-only preflight for explicit local Bronze CSV inputs."""

import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import stat

from collectors.contracts import (
    ENRICHED_GAME_COLUMNS,
    POPULARITY_COLUMNS,
    REVIEW_COLUMNS,
)
from LakeHouse_Project.scripts.bronze_appid import canonical_appid


REQUIRED_COLUMNS = {
    "games.csv": frozenset(ENRICHED_GAME_COLUMNS),
    "reviews.csv": frozenset(REVIEW_COLUMNS),
    "popularity.csv": frozenset(POPULARITY_COLUMNS),
}
NULL_MARKERS = {
    "", "#N/A", "#N/A N/A", "#NA", "-1.#IND", "-1.#QNAN", "-NaN", "-nan",
    "1.#IND", "1.#QNAN", "<NA>", "N/A", "NA", "NULL", "NaN", "n/a", "nan", "null",
}
ISO_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
    r"(?:[ T][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.(?P<fraction>[0-9]{1,9}))?"
    r"(?:Z|[+-](?:[01][0-9]|2[0-3]):?[0-5][0-9])?)?"
)
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
CSV_ISSUES = (
    "missing_file", "read_errors", "parse_errors", "missing_header", "blank_headers",
    "duplicate_headers", "missing_required_columns", "bad_width_rows",
    "invalid_field_rows", "missing_appids", "invalid_appids", "duplicate_appids",
    "duplicate_appid_rows", "missing_timestamps", "non_iso_timestamps",
    "invalid_timestamps",
)


def _timestamp_issue(value):
    value = value.strip()
    if value in NULL_MARKERS:
        return "missing_timestamps"
    match = ISO_TIMESTAMP.fullmatch(value)
    if not match:
        return "non_iso_timestamps"
    try:
        timestamp = datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        elapsed = timestamp - EPOCH
        fraction = (match["fraction"] or "").ljust(9, "0")
        nanos = (elapsed.days * 86_400 + elapsed.seconds) * 10**9 + int(fraction)
        if -(2**63) < nanos <= 2**63 - 1:
            return None
    except (ValueError, OverflowError):
        pass
    return "invalid_timestamps"


def _csv_counts():
    return dict.fromkeys((*CSV_ISSUES, "rows", "extra_columns", "empty_tags"), 0)


def _scan_csv(stream, name, counts):
    """Count issues and return header names for internal comparison only."""
    required = REQUIRED_COLUMNS[name]
    timestamp_col = "ts" if name == "popularity.csv" else "ts_ingested"
    rows = (row for row in csv.reader(stream, strict=True) if row)
    header = next(rows, [])
    if not header:
        counts["missing_header"] = 1
        return None
    columns = frozenset(header)
    counts["blank_headers"] = sum(not item.strip() for item in header)
    counts["duplicate_headers"] = len(header) - len(columns)
    counts["missing_required_columns"] = len(required - columns)
    counts["extra_columns"] = len(columns - required)
    if any("\x00" in item for item in header):
        counts["parse_errors"] += 1
        return None
    if counts["blank_headers"] or counts["duplicate_headers"]:
        return None

    seen = set()
    duplicated = set()
    for fields in rows:
        counts["rows"] += 1
        if len(fields) != len(header):
            counts["bad_width_rows"] += 1
            continue
        if any("\x00" in value for value in fields):
            counts["invalid_field_rows"] += 1
            continue

        row = dict(zip(header, fields))
        if "steam_appid" in row:
            if row["steam_appid"].strip() in NULL_MARKERS:
                counts["missing_appids"] += 1
            else:
                try:
                    key = canonical_appid(row["steam_appid"])
                except ValueError:
                    counts["invalid_appids"] += 1
                else:
                    if key in seen:
                        counts["duplicate_appid_rows"] += 1
                        duplicated.add(key)
                    seen.add(key)

        if timestamp_col in row:
            issue = _timestamp_issue(row[timestamp_col])
            if issue:
                counts[issue] += 1
        if name == "games.csv" and "tags" in row:
            tags = row["tags"].strip()
            if tags in NULL_MARKERS or not any(tag.strip() for tag in tags.split(",")):
                counts["empty_tags"] += 1
    counts["duplicate_appids"] = len(duplicated)
    return columns


def _is_link(metadata):
    return stat.S_ISLNK(metadata.st_mode) or bool(
        getattr(metadata, "st_file_attributes", 0) & stat.FILE_ATTRIBUTE_REPARSE_POINT
    )


def _inventory(root):
    counts = dict.fromkeys(("directory_errors", "tags_sidecars", "other_csv", "other_files",
                            "non_regular_entries"), 0)
    expected = {}
    try:
        metadata = root.lstat()
        if _is_link(metadata) or not stat.S_ISDIR(metadata.st_mode):
            counts["directory_errors"] = 1
            return counts, expected
        with os.scandir(root) as entries:
            for entry in entries:
                metadata = entry.stat(follow_symlinks=False)
                if _is_link(metadata) or not stat.S_ISREG(metadata.st_mode):
                    counts["non_regular_entries"] += 1
                elif entry.name in REQUIRED_COLUMNS:
                    expected[entry.name] = Path(entry.path)
                elif entry.name.casefold() == "game_tags.csv":
                    counts["tags_sidecars"] += 1
                elif entry.name.casefold().endswith(".csv"):
                    counts["other_csv"] += 1
                else:
                    counts["other_files"] += 1
    except (OSError, ValueError):
        counts["directory_errors"] += 1
    return counts, expected


def _scan_file(path, name, *, source):
    counts = _csv_counts()
    columns = None
    if path is None:
        counts["missing_file"] = 1
    else:
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                columns = _scan_csv(stream, name, counts)
        except (UnicodeError, csv.Error):
            counts["parse_errors"] += 1
        except (OSError, ValueError):
            counts["read_errors"] += 1
    issues = set(CSV_ISSUES)
    if source:
        issues.difference_update({"duplicate_appids", "duplicate_appid_rows"})
    counts["status"] = "FAIL" if any(counts[key] for key in issues) else "PASS"
    if name == "games.csv":
        counts["tags_quality_status"] = "FAIL" if counts["empty_tags"] else "PASS"
    return counts, columns


def _scan_directory(root, *, source):
    inventory, expected = _inventory(root)
    datasets = {}
    headers = {}
    for name in REQUIRED_COLUMNS:
        datasets[name], headers[name] = _scan_file(expected.get(name), name, source=source)
    failed = inventory["directory_errors"] or any(report["status"] == "FAIL" for report in datasets.values())
    return {"status": "FAIL" if failed else "PASS", "inventory": inventory,
            "datasets": datasets}, headers


def preflight(source_dir, target_dir=None):
    """Check explicit local directories without writing or opening auxiliary files."""
    source, source_headers = _scan_directory(Path(source_dir), source=True)
    report = {"status": source["status"], "target_checked": target_dir is not None, "source": source}
    if target_dir is None:
        return report

    target, target_headers = _scan_directory(Path(target_dir), source=False)
    comparison = {}
    for name in REQUIRED_COLUMNS:
        left, right = source_headers[name], target_headers[name]
        if left is None or right is None:
            comparison[name] = {"status": "FAIL", "compared": False}
        else:
            comparison[name] = {"status": "PASS" if left == right else "FAIL", "compared": True,
                                "source_only_columns": len(left - right),
                                "target_only_columns": len(right - left)}
    report["target"] = target
    report["schema_comparison"] = comparison
    if target["status"] == "FAIL" or any(item["status"] == "FAIL" for item in comparison.values()):
        report["status"] = "FAIL"
    return report


class _SafeParser(argparse.ArgumentParser):
    def error(self, message):
        raise ValueError("Invalid preflight arguments")


def main(argv=None):
    parser = _SafeParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--source-dir", required=True, type=Path)
    parser.add_argument("--target-dir", type=Path)
    try:
        args = parser.parse_args(argv)
        report = preflight(args.source_dir, args.target_dir)
    except ValueError:
        report = {"status": "FAIL", "argument_errors": 1}
    print(json.dumps(report, sort_keys=True))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
