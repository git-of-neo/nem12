# Using this as a util to check DB content because i dont have sqlite programme setup locally
"""
query.py — Manual sanity-check viewer for meter_readings.db.

Run after `go run . data/sample.NEM12` to visually verify output

Usage:
  python query.py [path/to/meter_readings.db]
"""

import sqlite3
import sys

DB_PATH = sys.argv[1] if len(sys.argv) > 1 else "meter_readings.db"


def hr(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


con = sqlite3.connect(DB_PATH)
cur = con.cursor()


# ---------------------------------------------------------------------------
hr(
    """First 5 timestamps for NEM1201009
    1. verify 30-min spacing
    2. NEM1201009 nmi entry should exist (this query should work)
    3. date should start from 20050301
    """
)

cur.execute("""
    SELECT   timestamp, consumption
    FROM     meter_readings
    WHERE    nmi = 'NEM1201009'
    ORDER BY timestamp
    LIMIT    5
""")
print(f"  {'timestamp':<25} {'consumption':>12}")
print(f"  {'-' * 25} {'-' * 12}")
for ts, consumption in cur.fetchall():
    print(f"  {ts:<25} {consumption:>12.3f}")

# ---------------------------------------------------------------------------
hr("First 15 intervals for NEM1201009 on 2005-03-01 (First non zero should be 0.461)")

cur.execute("""
    SELECT   timestamp, consumption
    FROM     meter_readings
    WHERE    nmi       = 'NEM1201009'
      AND    timestamp >= '2005-03-01T00:00:00'
      AND    timestamp  < '2005-03-02T00:00:00'
    ORDER BY timestamp
    LIMIT    15
""")
print(f"  {'timestamp':<25} {'consumption':>12}")
print(f"  {'-' * 25} {'-' * 12}")
for ts, consumption in cur.fetchall():
    marker = "  <-- first non-zero" if consumption == 0.461 else ""
    print(f"  {ts:<25} {consumption:>12.3f}{marker}")

con.close()
print()
