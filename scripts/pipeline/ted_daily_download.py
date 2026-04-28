#!/usr/bin/env python3
"""
Pobieranie dziennej paczki XML z TED po dacie publikacji.

Jak to działa:
1. Pobiera oficjalny release calendar TED w CSV dla danego roku.
2. Zamienia datę publikacji (YYYY-MM-DD) na numer OJ S.
3. Buduje URL dziennej paczki TED i pobiera archiwum.
4. Opcjonalnie rozpakowuje XML-e.

Przykłady:
    python ted_daily_download.py 2026-04-07
    python ted_daily_download.py 2026-04-07 --out-dir data/ted --extract
"""

from __future__ import annotations

import argparse
import csv
import io
import re
import sys
import zipfile
from datetime import datetime
from pathlib import Path

import requests

RELEASE_CALENDAR_URL = "https://ted.europa.eu/en/release-calendar/-/download/file/CSV/{year}"
DAILY_PACKAGE_URL = "https://ted.europa.eu/packages/daily/{package_id}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pobierz dzienną paczkę XML z TED po dacie.")
    parser.add_argument(
        "date",
        help="Data publikacji w formacie YYYY-MM-DD, np. 2026-04-07",
    )
    parser.add_argument(
        "--out-dir",
        default=".",
        help="Katalog docelowy na pobraną paczkę i ewentualne rozpakowanie (domyślnie: bieżący katalog).",
    )
    parser.add_argument(
        "--extract",
        action="store_true",
        help="Jeśli podane, skrypt rozpakowuje pobrane archiwum ZIP.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=120,
        help="Timeout HTTP w sekundach (domyślnie: 120).",
    )
    return parser.parse_args()


def parse_input_date(date_str: str) -> datetime.date:
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as e:
        raise SystemExit(f"Błędny format daty: {date_str}. Oczekiwano YYYY-MM-DD.") from e


def fetch_release_calendar_csv(year: int, timeout: int) -> str:
    url = RELEASE_CALENDAR_URL.format(year=year)
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def find_ojs_number_for_date(csv_text: str, target_date) -> int:
    """
    CSV ma prostą postać:
    OJS,Publication date
    1,02/01/2026
    2,05/01/2026
    ...
    """
    reader = csv.DictReader(io.StringIO(csv_text))
    if reader.fieldnames is not None:
        reader.fieldnames = [field.strip() if field is not None else field for field in reader.fieldnames]
    target_str = target_date.strftime("%d/%m/%Y")

    for row in reader:
        if row["Publication date"].strip() == target_str:
            return int(row["OJS"].strip())

    raise ValueError(f"Brak wydania OJ S dla daty {target_date.isoformat()}.")


def build_package_id(year: int, ojs_number: int) -> str:
    # Oficjalny format: {yyyynnnnn}, gdzie nnnnn to OJ S z zerami wiodącymi
    return f"{year}{ojs_number:05d}"


def get_filename_from_headers(response: requests.Response, fallback: str) -> str:
    cd = response.headers.get("Content-Disposition", "")
    match = re.search(r'filename="?([^"]+)"?', cd)
    if match:
        return match.group(1)
    return fallback


def download_daily_package(package_id: str, out_dir: Path, timeout: int) -> Path:
    url = DAILY_PACKAGE_URL.format(package_id=package_id)
    response = requests.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    fallback_name = f"ted_daily_{package_id}.zip"
    filename = get_filename_from_headers(response, fallback_name)
    output_path = out_dir / filename

    with output_path.open("wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

    return output_path


def extract_if_zip(archive_path: Path, out_dir: Path) -> Path | None:
    if not zipfile.is_zipfile(archive_path):
        return None

    extract_dir = out_dir / archive_path.stem
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(archive_path, "r") as zf:
        zf.extractall(extract_dir)

    return extract_dir


def main() -> int:
    args = parse_args()
    target_date = parse_input_date(args.date)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        csv_text = fetch_release_calendar_csv(target_date.year, args.timeout)
        ojs_number = find_ojs_number_for_date(csv_text, target_date)
        package_id = build_package_id(target_date.year, ojs_number)
        archive_path = download_daily_package(package_id, out_dir, args.timeout)

        print(f"Data publikacji : {target_date.isoformat()}")
        print(f"Numer OJ S      : {ojs_number}")
        print(f"Package ID      : {package_id}")
        print(f"Zapisano        : {archive_path}")

        if args.extract:
            extracted_to = extract_if_zip(archive_path, out_dir)
            if extracted_to:
                print(f"Rozpakowano do  : {extracted_to}")
            else:
                print("Pobrany plik nie wygląda na ZIP — pomijam rozpakowanie.")

        return 0

    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else "?"
        print(f"Błąd HTTP podczas pobierania: {status}", file=sys.stderr)
        return 1
    except requests.RequestException as e:
        print(f"Błąd sieciowy: {e}", file=sys.stderr)
        return 1
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2
    except Exception as e:
        print(f"Nieoczekiwany błąd: {e}", file=sys.stderr)
        return 99


if __name__ == "__main__":
    raise SystemExit(main())
