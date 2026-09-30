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
Run SQL against Redis through the ADBC driver and print the result.

    uv run --project validation python examples/sql.py "SELECT COUNT(*) FROM sales"
    uv run --project validation python examples/sql.py      # interactive
"""

import sys

import pyarrow

from common import connect


def show(table: pyarrow.Table) -> None:
    names = table.column_names
    rows = [
        ["NULL" if v is None else str(v) for v in row.values()]
        for row in table.to_pylist()
    ]
    widths = [max([len(n)] + [len(r[i]) for r in rows]) for i, n in enumerate(names)]
    print(" | ".join(n.ljust(w) for n, w in zip(names, widths)))
    print("-+-".join("-" * w for w in widths))
    for r in rows:
        print(" | ".join(v.ljust(w) for v, w in zip(r, widths)))
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


def run(cur, sql: str) -> None:
    cur.execute(sql)
    table = cur.fetch_arrow_table()
    if table.num_columns:
        show(table)
    else:
        print("OK" if cur.rowcount < 0 else f"OK ({cur.rowcount} row(s) affected)")


def main() -> None:
    with connect() as conn, conn.cursor() as cur:
        if len(sys.argv) > 1:
            run(cur, " ".join(sys.argv[1:]))
            return
        print("Enter SQL terminated by ';' (Ctrl-D to exit)")
        buf = []
        for line in sys.stdin:
            buf.append(line)
            if line.rstrip().endswith(";"):
                try:
                    run(cur, "".join(buf))
                except Exception as e:  # noqa: BLE001
                    print(f"error: {e}")
                buf = []


if __name__ == "__main__":
    main()
