"""Generate sample partner files for the cold path (ADLS landing/partners/).

Writes one promotions CSV and one footfall JSON per --date into --out, laid out
like the landing container, so it can be uploaded as-is:

    python scripts/generate_partner_files.py --date 2026-11-07
    az storage blob upload-batch --auth-mode login --account-name <acct> \
        -d landing -s sample_data/landing

A few rows deliberately break the DQ rules in metadata/config/entities/partner_files/
(use --clean to leave them out):
  promotions: a NULL promo_id and a 95% discount  -> quarantine
  footfall:   negative visitors -> quarantine, region "Central" -> warn
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

STORES = [
    (1, "Downtown", "North"), (2, "Riverside", "North"), (3, "Harbor", "East"),
    (4, "Airport", "East"), (5, "Old Town", "South"), (6, "Lakeside", "South"),
    (7, "Uptown", "West"), (8, "Hillcrest", "West"), (9, "Mall Plaza", "North"),
    (10, "Station", "East"),
]
STOCK_ITEM_IDS = range(1, 228)  # WWI Warehouse.StockItems


def promotions(day: date, rng: random.Random, clean: bool) -> list[dict]:
    rows = [
        {
            "promo_id": f"P{day:%Y%m%d}-{i:03d}",
            "stock_item_id": rng.choice(STOCK_ITEM_IDS),
            "discount_pct": f"{rng.choice([5, 10, 15, 20, 25, 30]):.2f}",
            "start_date": day.isoformat(),
            "end_date": (day + timedelta(days=6)).isoformat(),
        }
        for i in range(1, 21)
    ]
    if not clean:
        rows[3]["promo_id"] = ""          # NULL promo_id
        rows[7]["discount_pct"] = "95.00"  # out of range 0-90
    return rows


def footfall(day: date, rng: random.Random, clean: bool) -> list[dict]:
    rows = [
        {"store_id": sid, "store_name": name, "region": region, "date": day.isoformat(),
         "visitors": rng.randint(300, 2500)}
        for sid, name, region in STORES
    ]
    if not clean:
        rows[2]["visitors"] = -40      # quarantine
        rows[5]["region"] = "Central"  # warn
    return rows


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", type=date.fromisoformat, default=date.today())
    p.add_argument("--days", type=int, default=1, help="number of consecutive days to generate")
    p.add_argument("--out", type=Path, default=Path("sample_data/landing"))
    p.add_argument("--clean", action="store_true", help="omit the DQ-breaking rows")
    args = p.parse_args()

    for n in range(args.days):
        day = args.date + timedelta(days=n)
        rng = random.Random(day.toordinal())

        promo_dir = args.out / "partners" / "promotions"
        promo_dir.mkdir(parents=True, exist_ok=True)
        promo_file = promo_dir / f"promotions_{day:%Y%m%d}.csv"
        rows = promotions(day, rng, args.clean)
        with promo_file.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

        foot_dir = args.out / "partners" / "footfall"
        foot_dir.mkdir(parents=True, exist_ok=True)
        foot_file = foot_dir / f"footfall_{day:%Y%m%d}.json"
        foot_file.write_text(json.dumps(footfall(day, rng, args.clean), indent=2), encoding="utf-8")

        print(f"wrote {promo_file} and {foot_file}")


if __name__ == "__main__":
    main()
