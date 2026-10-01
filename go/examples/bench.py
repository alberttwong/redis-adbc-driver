# Copyright (c) 2026 ADBC Drivers Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Query benchmark on the sample data set.

Loads `sales` (bulk ingest) and `customers`, then runs a set of
representative queries several times each, reporting wall time (including
Arrow transfer to Python) and the Redis commands each query sends (from
INFO commandstats, summed over all cluster nodes).

    uv run --project validation --with redis python examples/bench.py --rows 100000
"""

import argparse
import os
import statistics
import time
from urllib.parse import urlparse

import redis

import load_sample
from common import connect

QUERIES = [
    ("point lookup by __rowid", "SELECT * FROM sales WHERE __rowid = 4242"),
    ("indexed filter + top 10",
     "SELECT order_id, product, quantity FROM sales WHERE country = 'JPN' AND status = 'shipped' "
     "ORDER BY quantity DESC LIMIT 10"),
    ("indexed range COUNT", "SELECT COUNT(*) FROM sales WHERE quantity BETWEEN 5 AND 10"),
    ("LIKE prefix COUNT", "SELECT COUNT(*) FROM sales WHERE product LIKE 'gi%'"),
    ("interval range COUNT",
     "SELECT COUNT(*) FROM sales WHERE ordered_at >= TIMESTAMP '2025-12-31 00:00:00' - INTERVAL '30 days'"),
    ("GROUP BY, index reduce", "SELECT country, COUNT(*), SUM(quantity) FROM sales GROUP BY country"),
    ("GROUP BY, driver (AVG double)", "SELECT country, AVG(discount) FROM sales GROUP BY country"),
    ("GROUP BY expression, driver", "SELECT product, SUM(quantity * unit_price) FROM sales GROUP BY product"),
    ("GROUP BY DATE_TRUNC, driver",
     "SELECT DATE_TRUNC('month', ordered_at) AS m, COUNT(*) FROM sales GROUP BY m ORDER BY m"),
    ("non-indexed filter (scan)", "SELECT COUNT(*) FROM sales WHERE notes LIKE '%order 1%'"),
    ("fetch 10% of rows", "SELECT order_id, country, quantity FROM sales WHERE order_id <= {tenth}"),
    ("full scan, all columns", "SELECT * FROM sales"),
    ("selective join (index lookup)",
     "SELECT c.name, COUNT(*) FROM sales s JOIN customers c ON s.customer_id = c.customer_id "
     "WHERE c.country = 'JPN' AND s.status = 'shipped' GROUP BY c.name"),
    ("unfiltered join (hash join)",
     "SELECT c.country, COUNT(*) FROM sales s JOIN customers c ON s.customer_id = c.customer_id GROUP BY c.country"),
    ("correlated subquery (50 rows)",
     "SELECT c.name, (SELECT COUNT(*) FROM sales s WHERE s.customer_id = c.customer_id) FROM customers c"),
    ("UNION of two filters",
     "SELECT order_id FROM sales WHERE country = 'JPN' AND quantity = 20 "
     "UNION SELECT order_id FROM sales WHERE product = 'gizmo' AND quantity = 20"),
    ("view with pushed filter", "SELECT COUNT(*) FROM bench_shipped WHERE country = 'JPN' AND quantity >= 15"),
]


def redis_nodes(uri: str) -> list[redis.Redis]:
    """One client per node (all masters of a cluster, or the single server)."""
    u = urlparse(uri)
    kw = dict(host=u.hostname, port=u.port or 6379, password=u.password, username=u.username or None,
              ssl=u.scheme == "rediss", decode_responses=True)
    first = redis.Redis(**kw)
    if first.info("cluster").get("cluster_enabled") != 1:
        return [first]
    nodes = []
    for line in first.execute_command("CLUSTER", "NODES").splitlines():
        parts = line.split()
        if "master" in parts[2]:
            host, port = parts[1].split("@")[0].rsplit(":", 1)
            nodes.append(redis.Redis(**{**kw, "host": host, "port": int(port)}))
    return nodes


def command_counts(nodes: list[redis.Redis]) -> dict[str, int]:
    total: dict[str, int] = {}
    for n in nodes:
        for k, v in n.info("commandstats").items():
            name = k.removeprefix("cmdstat_")
            total[name] = total.get(name, 0) + v["calls"]
    return total


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--repeat", type=int, default=5)
    ap.add_argument("--skip-load", action="store_true")
    args = ap.parse_args()
    uri = os.environ.get("REDIS_URI", "redis://localhost:6379/0")
    nodes = redis_nodes(uri)
    print(f"server: {uri.split('@')[-1]}  nodes: {len(nodes)}  rows: {args.rows}  repeats: {args.repeat}\n")

    with connect() as conn, conn.cursor() as cur:
        if not args.skip_load:
            cur.execute("DROP TABLE IF EXISTS customers")
            cur.execute("CREATE TABLE customers (customer_id INTEGER NOT NULL, name VARCHAR, country VARCHAR)")
            cur.execute("INSERT INTO customers VALUES " + ", ".join(
                f"({i}, 'Customer {i}', '{load_sample.COUNTRIES[i % 6]}')" for i in range(1, 51)))
            table = load_sample.sales_table(args.rows)
            cur.adbc_statement.set_options(**{"adbc.redis.ingest.index_columns":
                "order_id,customer_id,country,product,quantity,unit_price,discount,ordered_at,status"})
            t0 = time.perf_counter()
            cur.adbc_ingest("sales", table, mode="replace")
            dt = time.perf_counter() - t0
            print(f"bulk ingest: {args.rows} rows x {table.num_columns} columns in {dt:.2f}s "
                  f"({args.rows / dt:,.0f} rows/s)\n")
        cur.execute("CREATE OR REPLACE VIEW bench_shipped AS "
                    "SELECT order_id, country, quantity FROM sales WHERE status = 'shipped'")

        header = f"{'query':34} {'rows':>7} {'min ms':>9} {'median':>9} {'max':>9}  redis commands per run"
        print(header)
        print("-" * len(header))
        for name, sql in QUERIES:
            sql = sql.format(tenth=args.rows // 10)
            cur.execute(sql)
            cur.fetch_arrow_table()  # warm-up (and connection/metadata caches)
            before = command_counts(nodes)
            times, nrows = [], 0
            for _ in range(args.repeat):
                t0 = time.perf_counter()
                cur.execute(sql)
                nrows = cur.fetch_arrow_table().num_rows
                times.append((time.perf_counter() - t0) * 1000)
            after = command_counts(nodes)
            delta = {k: (after.get(k, 0) - before.get(k, 0)) / args.repeat for k in after}
            delta.pop("info", None)
            cmds = sorted(((v, k) for k, v in delta.items() if v >= 0.5), reverse=True)
            total = sum(v for v, _ in cmds)
            detail = ", ".join(f"{k} {v:,.0f}" for v, k in cmds[:3])
            print(f"{name:34} {nrows:>7} {min(times):>9.1f} {statistics.median(times):>9.1f} "
                  f"{max(times):>9.1f}  {total:,.0f} ({detail})")
        cur.execute("DROP VIEW bench_shipped")


if __name__ == "__main__":
    main()
