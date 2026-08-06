#!/usr/bin/env python3
"""
Inspeciona as camadas do warehouse sem precisar abrir o psql.

    pip install psycopg2-binary
    python scripts/inspect_warehouse.py [--host localhost] [--port 5433]

A primeira tabela impressa é a control.v_layer_counts, o atalho da demo de
idempotência: reexecutar a mesma run não pode mexer nesses números, tirando
o que a origem tenha mudado nesse meio tempo.
"""

from __future__ import annotations

import argparse

import psycopg2
import psycopg2.extras


def _connect(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def _print_table(title: str, rows: list[dict]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("(vazio)")
        return
    headers = list(rows[0].keys())
    widths = {h: max(len(h), max(len(str(r[h])) for r in rows)) for h in headers}
    print(" | ".join(h.ljust(widths[h]) for h in headers))
    print("-+-".join("-" * widths[h] for h in headers))
    for r in rows:
        print(" | ".join(str(r[h]).ljust(widths[h]) for h in headers))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", default="5433")
    parser.add_argument("--dbname", default="hackernewsdb")
    parser.add_argument("--user", default="hackernews")
    parser.add_argument("--password", default="hackernews123")
    args = parser.parse_args()

    conn = _connect(args)
    conn.autocommit = True
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute("SELECT layer, row_count FROM control.v_layer_counts")
        _print_table("Contagem por camada", cur.fetchall())

        cur.execute(
            """
            SELECT run_id, status, started_at, finished_at,
                   ids_discovered, items_fetched, bronze_inserted, silver_upserted
            FROM control.run_log
            ORDER BY started_at DESC
            LIMIT 10
            """
        )
        _print_table("Últimas 10 execuções", cur.fetchall())

        cur.execute(
            """
            SELECT check_name, passed, observed
            FROM control.dq_results
            WHERE run_id = (SELECT run_id FROM control.run_log ORDER BY started_at DESC LIMIT 1)
            ORDER BY check_name
            """
        )
        _print_table("Quality checks da última execução", cur.fetchall())

        cur.execute(
            """
            SELECT rank_position, title, domain, score, descendants, score_per_hour
            FROM gold.current_ranking
            ORDER BY rank_position
            LIMIT 10
            """
        )
        _print_table("Top 10 do ranking atual", cur.fetchall())

        cur.execute(
            """
            SELECT item_id, title, score, domain, version_count
            FROM silver.v_most_revised
            LIMIT 10
            """
        )
        _print_table("Stories mais revisadas (mais versões no Bronze)", cur.fetchall())

        cur.close()
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
