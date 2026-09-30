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
Load the sample data set: a `customers` table created with SQL and a
`sales` table bulk-ingested from Arrow.

    uv run --project validation python examples/load_sample.py [--rows N]
"""

import argparse
import datetime
import decimal
import random

import pyarrow

from common import connect

COUNTRIES = ["USA", "CAN", "MEX", "GBR", "DEU", "JPN"]
PRODUCTS = ["widget", "gadget", "gizmo", "doohickey", "sprocket"]
STATUSES = ["shipped", "pending", "returned"]


def sales_table(rows: int) -> pyarrow.Table:
    rng = random.Random(42)
    start = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
    data = {
        "order_id": list(range(1, rows + 1)),
        "customer_id": [rng.randint(1, 50) for _ in range(rows)],
        "country": [rng.choice(COUNTRIES) for _ in range(rows)],
        "product": [rng.choice(PRODUCTS) for _ in range(rows)],
        "quantity": [rng.randint(1, 20) for _ in range(rows)],
        "unit_price": [
            decimal.Decimal(rng.randint(199, 9999)) / 100 for _ in range(rows)
        ],
        "discount": [
            None if rng.random() < 0.3 else round(rng.random() * 0.25, 3)
            for _ in range(rows)
        ],
        "ordered_at": [
            start + datetime.timedelta(minutes=rng.randint(0, 525_600))
            for _ in range(rows)
        ],
        "status": [rng.choice(STATUSES) for _ in range(rows)],
        "notes": [
            None if rng.random() < 0.8 else f"gift wrap order {i}"
            for i in range(rows)
        ],
    }
    schema = pyarrow.schema(
        [
            ("order_id", pyarrow.int64()),
            ("customer_id", pyarrow.int32()),
            ("country", pyarrow.string()),
            ("product", pyarrow.string()),
            ("quantity", pyarrow.int32()),
            ("unit_price", pyarrow.decimal128(10, 2)),
            ("discount", pyarrow.float64()),
            ("ordered_at", pyarrow.timestamp("us", tz="UTC")),
            ("status", pyarrow.string()),
            ("notes", pyarrow.string()),
        ]
    )
    return pyarrow.Table.from_pydict(data, schema=schema)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    args = parser.parse_args()

    with connect() as conn, conn.cursor() as cur:
        # customers: created and filled with plain SQL. `email` is NOINDEX:
        # stored in each row HASH but left out of the RediSearch index.
        cur.execute("DROP TABLE IF EXISTS customers")
        cur.execute(
            "CREATE TABLE customers ("
            " customer_id INTEGER NOT NULL,"
            " name VARCHAR,"
            " country VARCHAR,"
            " signup_date DATE,"
            " email VARCHAR NOINDEX)"
        )
        rng = random.Random(7)
        values = ", ".join(
            f"({i}, 'Customer {i}', '{rng.choice(COUNTRIES)}',"
            f" DATE '2024-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}',"
            f" 'customer{i}@example.com')"
            for i in range(1, 51)
        )
        cur.execute(f"INSERT INTO customers VALUES {values}")

        # sales: bulk ingest from Arrow. Only the columns we filter, sort or
        # aggregate on are indexed; `notes` lives only in the row HASHes.
        cur.adbc_statement.set_options(
            **{
                "adbc.redis.ingest.index_columns": (
                    "order_id,customer_id,country,product,quantity,"
                    "unit_price,discount,ordered_at,status"
                )
            }
        )
        n = cur.adbc_ingest("sales", sales_table(args.rows), mode="replace")
        print(f"loaded 50 customers and {n} sales rows")


if __name__ == "__main__":
    main()
