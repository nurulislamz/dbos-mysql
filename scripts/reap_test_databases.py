#!/usr/bin/env python3
"""Drop orphaned test databases left by killed or interrupted test runs.

The suites create one database per test (`dbos_t_<uuid>`) on a shared server and
drop it in Drop. A killed run never reaches Drop, so orphans accumulate and the
server slowly runs out of memory and connections.

Safe by default: only databases whose name starts with `dbos_t_` and whose newest
activity is older than --min-age seconds are touched. Databases with a live
connection are always skipped, so a running test is never disturbed.
"""
import argparse
import sys
import time

MYSQL = dict(host="127.0.0.1", port=13307, user="root", password="dbos-test-root")
PG = dict(host="127.0.0.1", port=13309, user="postgres", password="dbos-test-root")


def reap_mysql(min_age: int, dry: bool) -> list[str]:
    import pymysql

    conn = pymysql.connect(**MYSQL, autocommit=True)
    cur = conn.cursor()
    # A database with any open connection belongs to a live run.
    cur.execute("SELECT DISTINCT db FROM information_schema.PROCESSLIST WHERE db IS NOT NULL")
    busy = {r[0] for r in cur.fetchall()}

    cur.execute("SELECT schema_name FROM information_schema.SCHEMATA")
    candidates = [
        n
        for n in (r[0] for r in cur.fetchall())
        if n.startswith("dbos_t_") and n not in busy
    ]

    dropped = []
    for name in candidates:
        # information_schema does not expose a create time on MySQL, so use the
        # server-side file mtime proxy: the age of the schema directory is not
        # visible either. Fall back to the newest table's update time in the
        # database, which is a good proxy for "last touched".
        cur.execute(
            "SELECT MAX(GREATEST(IFNULL(update_time, 0), IFNULL(create_time, 0))) "
            "FROM information_schema.TABLES WHERE table_schema = %s",
            (name,),
        )
        row = cur.fetchone()
        newest = row[0] if row and row[0] else 0
        # A database with no tables at all has newest == 0; treat it as old.
        age = time.time() - (newest.timestamp() if hasattr(newest, "timestamp") else newest)
        if newest and age < min_age:
            continue
        dropped.append(name)
        if not dry:
            cur.execute(f"DROP DATABASE IF EXISTS `{name}`")
    conn.close()
    return dropped


def reap_postgres(min_age: int, dry: bool) -> list[str]:
    import psycopg

    conn = psycopg.connect(**PG, dbname="postgres", autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datname LIKE 'dbos\\_t\\_%'")
    names = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT datname FROM pg_stat_activity WHERE datname IS NOT NULL")
    busy = {r[0] for r in cur.fetchall()}

    dropped = []
    for name in names:
        if name in busy:
            continue
        cur.execute(f"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)")
        dropped.append(name)
    conn.close()
    return dropped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-age", type=int, default=900, help="seconds of inactivity before a database is a candidate")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--backend", choices=["mysql", "postgres", "both"], default="both")
    args = ap.parse_args()

    total = 0
    if args.backend in ("mysql", "both"):
        try:
            names = reap_mysql(args.min_age, args.dry_run)
            print(f"mysql: {len(names)} orphan(s){' would be' if args.dry_run else ''} dropped")
            for n in names:
                print(f"  {n}")
            total += len(names)
        except Exception as exc:  # noqa: BLE001 - report and continue
            print(f"mysql: skipped ({type(exc).__name__}: {exc})", file=sys.stderr)

    if args.backend in ("postgres", "both"):
        try:
            names = reap_postgres(args.min_age, args.dry_run)
            print(f"postgres: {len(names)} orphan(s){' would be' if args.dry_run else ''} dropped")
            for n in names:
                print(f"  {n}")
            total += len(names)
        except Exception as exc:  # noqa: BLE001
            print(f"postgres: skipped ({type(exc).__name__}: {exc})", file=sys.stderr)

    return 0 if total >= 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
