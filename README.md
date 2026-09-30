<!--
  Copyright (c) 2026 ADBC Drivers Contributors

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

          http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC Driver for Redis

An [ADBC](https://arrow.apache.org/adbc/) driver for Redis 8.4+, built on
[driverbase-go](https://github.com/adbc-drivers/driverbase-go) and validated
with the [ADBC driver validation suite](https://github.com/adbc-drivers/validation).
It supports SQL queries and Arrow bulk ingestion.

## Architecture: hybrid index-row layout

```
          ┌───────────────────────────────────────────────┐
          │ Secondary index (RediSearch)  idx:<s>:<t>     │
          │ only filter/sort/aggregate columns, SORTABLE  │
          └───────────────┬───────────────────────────────┘
      finds matching keys │   computes GROUPBY/REDUCE without
      (filter, sort,      │   opening the HASHes
      limit)              ▼
┌──────────────────────────────────────────────────────────────────┐
│ Primary data store: one flat HASH per row (every column)         │
│ public:sales:1 ─► { __rowid: 1, country: "USA", amount: 45, ... }│
└──────────────────────────────▲───────────────────────────────────┘
                               │ direct HMGET (full-row lookup)
```

- **Rows** are flat HASHes `<schema>:<table>:<rowid>` holding every column
  (NULL = field absent; a hidden `__rowid` field keeps all-NULL rows alive).
- **Selective index**: `FT.CREATE idx:<schema>:<table> ON HASH PREFIX 1
  <schema>:<table>:` covering only filterable columns: numeric, boolean,
  decimal, date/time and timestamp columns as `NUMERIC SORTABLE`, strings as
  `TAG CASESENSITIVE INDEXEMPTY SORTABLE UNF`. Binary columns and columns
  declared `NOINDEX` are stored in the HASH only.
- **Metadata**: `adbc:meta:<schema>:<table>` (column types as JSON),
  `adbc:seq:*` (row ids), `adbc:tables:<schema>`, `adbc:schemas`.

How SQL is executed:

| Query shape | Execution |
|-|-|
| `WHERE __rowid = N` | Direct `HMGET` of the row HASH, index bypassed |
| Filter / sort / limit | `FT.AGGREGATE <idx> "<pushed-down query>" LOAD 1 @__key SORTBY … LIMIT … WITHCURSOR`, then pipelined `HMGET` of only the needed columns |
| `COUNT(*)`, `GROUP BY` + `COUNT/SUM/AVG/MIN/MAX` | `FT.AGGREGATE … APPLY exists(@c) … GROUPBY … REDUCE …` over SORTABLE fields, HASHes never opened |
| Anything the index can't answer exactly | Evaluated by the driver on rows fetched from the HASHes |

Pushed down into the index: numeric range/equality predicates on indexed
columns (`@c:[lo hi]`), string equality on indexed columns (`@c:{value}`),
`ORDER BY` on indexed columns, `LIMIT/OFFSET`, and aggregates. Row values are
always read from the HASHes, never from the index sort vectors (those hold
doubles, which would round 64-bit integers, timestamps and decimals).

Aggregate pushdown (`adbc.redis.aggregate_pushdown`):

- `exact` (default): pushes COUNT, plus SUM/AVG/MIN/MAX over 16/32-bit
  integer, boolean and date columns, which RediSearch returns exactly. Other
  aggregates are computed by the driver.
- `all`: also pushes floating-point, decimal and 64-bit aggregates.
  RediSearch reports these with 12 significant digits.
- `none`: always aggregates in the driver.

## Supported SQL

- `CREATE TABLE [IF NOT EXISTS] t (col TYPE [NOT NULL] [NOINDEX], …)`,
  `DROP TABLE [IF EXISTS] t`, `CREATE/DROP SCHEMA`
- `INSERT INTO t [(cols)] VALUES (…), (…)` with literals or `?` / `$n` parameters
- `SELECT … FROM t [WHERE …] [GROUP BY …] [ORDER BY …] [LIMIT n] [OFFSET m]`,
  with `COUNT/SUM/AVG/MIN/MAX`, arithmetic, `CAST`, `IS [NOT] NULL`,
  `BETWEEN`, `IN`, `COALESCE`, `LOWER/UPPER/LENGTH/ABS/CONCAT`, `from_hex`
- `SELECT` without `FROM` for literal expressions
- `UPDATE t SET … [WHERE …]`, `DELETE FROM t [WHERE …]`
- Types: `BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE PRECISION,
  NUMERIC(p,s), VARCHAR/TEXT, VARBINARY/BLOB, DATE, TIME(p), TIMESTAMP(p)
  [WITH TIME ZONE]`

Tables can be qualified as `schema.table` or `redis.schema.table`. Schemas
are key namespaces (default `public`). There are no transactions (autocommit
only) and no joins.

## Options

| Option | Level | Meaning |
|-|-|-|
| `uri` | database | `redis://[user:pass@]host:port/db` |
| `username`, `password` | database | Credentials (override the URI) |
| `adbc.redis.address`, `adbc.redis.db` | database | Used when no URI is given |
| `adbc.redis.default_schema` | database | Schema for unqualified names (default `public`) |
| `adbc.redis.aggregate_pushdown` | database, statement | `exact` / `all` / `none` |
| `adbc.redis.ingest.index_columns` | statement | Comma-separated columns to index on bulk ingest (`*` = all indexable) |

Scale tips: keep column names short (they are repeated in every HASH), raise
`hash-max-listpack-entries` / `hash-max-listpack-value` so small rows use the
compact encoding, and index only the columns you filter or aggregate on.

## Building and testing

Requirements: Go 1.26+, a C toolchain (cgo), Docker, and [uv](https://docs.astral.sh/uv/).

```bash
cd go
docker compose up --detach --wait redis     # Redis 8.4
make build                                  # build/libadbc_driver_redis.{so,dylib}
cd validation
REDIS_URI=redis://localhost:6379/0 uv run pytest -v tests/
```

Using the driver from Python:

```python
import adbc_driver_manager.dbapi as dbapi
conn = dbapi.connect(driver="go/build/libadbc_driver_redis.so",
                     db_kwargs={"uri": "redis://localhost:6379/0"})
cur = conn.cursor()
cur.adbc_ingest("sales", arrow_table, mode="create")
cur.execute("SELECT country, SUM(amount) FROM sales WHERE amount > 10 GROUP BY country")
print(cur.fetch_arrow_table())
```

The C FFI shim in `go/pkg` was generated with driverbase-go's `ffitemplate`
(`go run main.go -prefix Redis -driver <path>/go`).
