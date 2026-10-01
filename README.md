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

An [ADBC](https://arrow.apache.org/adbc/) driver for Redis 8.x, built on
[driverbase-go](https://github.com/adbc-drivers/driverbase-go) and validated
with the [ADBC driver validation suite](https://github.com/adbc-drivers/validation).
It supports SQL queries and Arrow bulk ingestion.

## Tested with

The full validation suite (322 passed, 0 failed) has been run against:

| Server | Version | Connection |
|-|-|-|
| Redis Open Source (`redis:8.4` Docker image) | 8.4.4, Search 8.4.10 | `redis://` |
| Redis Open Source cluster, 3 shards (OSS Cluster API) | 8.4.4, Search 8.4.10 | `redis://` |
| Redis Cloud, single shard | 8.6.2, Search 8.6.10 | `redis://` and TLS (`rediss://`)* |
| Redis Cloud Pro, 2 shards, through the proxy endpoint | 8.6.2 | `redis://` |
| Redis Cloud Pro, 2 shards, OSS Cluster API enabled | 8.6.2 | `redis://` |

\* Run before cluster support was added; not re-run since.

The remaining 12 skipped tests and 1 expected failure are features the
driver doesn't offer: constraints, statistics, a second catalog, temporary
tables, transactions, and parameter-type introspection.

## Server requirements

You don't create any indexes or enable any settings yourself. The one hard
requirement is the Query Engine.

**What you need**

- **Redis with the Query Engine (RediSearch).** It is built into Redis 8.x
  (e.g. the `redis:8.4` image). On older versions use Redis Stack, or Redis
  Cloud / Redis Software with Search enabled. The driver runs `FT._LIST`
  when it connects and refuses to connect if search isn't available.
- **A standalone server, Redis Cloud / Redis Software, or a Redis Cluster.**
  The driver picks its client automatically (`adbc.redis.cluster=auto`):
  - If the server reports `cluster_enabled:1` (OSS Cluster API), it uses a
    cluster client. Rows spread across shards, and all search commands for a
    table go through one node, which coordinates the query across shards.
  - Otherwise it uses a single endpoint: standalone Redis, or Redis
    Cloud / Redis Software through their proxy.

  Driver metadata shares one hash slot (`adbc:{meta}:…`), so the driver's
  multi-key transactions never cross slots. A cluster only has database 0;
  on standalone servers only database 0 has been tested.
- **ACL permissions** (if ACLs are enabled):
  - commands: `FT.*`, `HSET/HMGET/HDEL/DEL`, `GET/SET/SETNX/EXISTS/INCRBY`,
    `SADD/SMEMBERS/SISMEMBER/SREM`, `MULTI/EXEC`, `INFO`, `PING`, `HELLO`,
    plus `CLUSTER` on an OSS Cluster API endpoint (to discover the shards)
  - keys: `adbc:*`, plus the row prefix of each schema you use (`public:*`
    by default)

**What the driver does for you**

- `CREATE TABLE` and bulk ingest create the table's index
  (`FT.CREATE idx:<schema>:<table> …`) and its metadata keys. `DROP TABLE`
  removes the index and all of the table's rows.
- Every filterable column is indexed by default. Narrow this with `NOINDEX`
  in `CREATE TABLE` or `adbc.redis.ingest.index_columns` on ingest. Queries
  still work on columns that aren't indexed: the driver scans the table's
  rows and filters them itself, which is slower on large tables but correct.
- Every query passes `TIMEOUT 0`, so the server's default search timeout
  (which can return partial results) doesn't apply. You don't need to change
  any `search-*` settings or enable keyspace notifications.

**Limitation: only tables created through the driver are visible**

A table exists for the driver only if its metadata key
(`adbc:{meta}:table:<schema>:<table>`) and its index (`idx:<schema>:<table>`) exist.
HASHes and indexes created some other way (for example your own
`row:*` / `idx:rows` layout) won't appear as tables. To query existing data,
load it through the driver; bulk ingest from Arrow is the quickest route.

## Quick start

Requirements: Go 1.26+, a C toolchain (cgo), Docker, and
[uv](https://docs.astral.sh/uv/). Run everything from the `go` directory.

**1. Start Redis 8.4 and build the driver**

```bash
cd go
docker compose up --detach --wait redis
make build
```

**2. Load the sample data set**

```bash
uv run --project validation python examples/load_sample.py
```

This creates two tables in the default `public` schema:

- `customers` (50 rows): created with `CREATE TABLE` / `INSERT`. `email` is
  declared `NOINDEX`, so it is stored in the row HASHes but not in the index.
- `sales` (10,000 rows, `--rows N` to change): bulk-ingested from an Arrow
  table with `order_id, customer_id, country, product, quantity,
  unit_price NUMERIC(10,2), discount, ordered_at TIMESTAMP WITH TIME ZONE,
  status, notes`. Every column except `notes` is indexed.

**3. Run SQL**

`examples/sql.py` runs one statement passed as an argument, or starts an
interactive prompt (statements end with `;`) when given none:

```bash
uv run --project validation python examples/sql.py "SELECT COUNT(*) FROM sales"
```

```bash
uv run --project validation python examples/sql.py
```

Common queries to try:

```sql
-- Browse rows (insertion order unless ORDER BY is given)
SELECT * FROM sales ORDER BY order_id LIMIT 5;

-- Count rows: GROUPBY 0 REDUCE COUNT inside the index
SELECT COUNT(*) FROM sales;

-- Filter on a TAG and a NUMERIC range, sort and limit inside the index,
-- then fetch just those 5 rows from their HASHes
SELECT order_id, country, product, quantity, unit_price
FROM sales
WHERE country = 'JPN' AND quantity >= 18
ORDER BY unit_price DESC
LIMIT 5;

-- Aggregate per group, computed from SORTABLE fields without opening HASHes
SELECT country, COUNT(*) AS orders, SUM(quantity) AS units, AVG(quantity) AS avg_units
FROM sales
GROUP BY country
ORDER BY units DESC;

-- Aggregate over an expression (computed exactly by the driver)
SELECT product, SUM(quantity * unit_price) AS revenue
FROM sales
WHERE status = 'shipped'
GROUP BY product
ORDER BY revenue DESC;

-- Timestamp range
SELECT order_id, ordered_at, status
FROM sales
WHERE ordered_at >= TIMESTAMP '2025-12-31 00:00:00'
ORDER BY ordered_at
LIMIT 5;

-- NULL handling
SELECT COUNT(*) AS no_discount FROM sales WHERE discount IS NULL;
SELECT order_id, notes FROM sales WHERE notes IS NOT NULL ORDER BY order_id LIMIT 3;

-- Full-row lookup by row id: a direct HMGET, the index is not used
SELECT __rowid, order_id, country, notes FROM sales WHERE __rowid = 42;

-- CASE expressions, also usable in GROUP BY (by alias or position)
SELECT order_id, quantity,
       CASE WHEN quantity >= 15 THEN 'bulk' WHEN quantity >= 5 THEN 'standard' ELSE 'small' END AS size
FROM sales ORDER BY order_id LIMIT 5;
SELECT CASE WHEN discount IS NULL THEN 'full price' ELSE 'discounted' END AS pricing, COUNT(*) AS orders
FROM sales GROUP BY pricing ORDER BY pricing;

-- HAVING filters groups; here the GROUPBY/REDUCE itself still runs in the index
SELECT country, COUNT(*) AS orders, SUM(quantity) AS units
FROM sales GROUP BY country HAVING SUM(quantity) > 17500 ORDER BY units DESC;

-- CREATE TABLE AS SELECT: column names and types come from the query
CREATE TABLE country_revenue AS
SELECT country, COUNT(*) AS orders, SUM(quantity * unit_price) AS revenue
FROM sales WHERE status = 'shipped' GROUP BY country;
SELECT * FROM country_revenue ORDER BY revenue DESC LIMIT 3;

-- Subqueries. An uncorrelated one runs once; its result is pushed into the
-- outer query's index filter (a range here, a union query for IN)
SELECT COUNT(*) AS above_avg FROM sales WHERE unit_price > (SELECT AVG(unit_price) FROM sales);
SELECT name, country FROM customers
WHERE customer_id IN (SELECT customer_id FROM sales WHERE quantity = 20 AND status = 'returned')
ORDER BY name LIMIT 5;

-- Correlated subqueries run per outer row (memoised on the outer values)
SELECT c.name, (SELECT COUNT(*) FROM sales s WHERE s.customer_id = c.customer_id) AS orders
FROM customers c ORDER BY orders DESC, c.name LIMIT 3;

-- CTEs and derived tables are computed once and held in memory
WITH per_customer AS (
  SELECT customer_id, SUM(quantity) AS units FROM sales GROUP BY customer_id
)
SELECT customer_id, units FROM per_customer
WHERE units > (SELECT AVG(units) FROM per_customer)
ORDER BY units DESC LIMIT 3;
SELECT bucket, COUNT(*) AS orders
FROM (SELECT CASE WHEN quantity >= 15 THEN 'bulk' ELSE 'regular' END AS bucket FROM sales) AS t
GROUP BY bucket ORDER BY bucket;

-- Joins. Each table's own filters run in its index; inner joins start from
-- the table with the fewest matching rows, then fetch only the matching rows
-- of the other (an index union on the join key)
SELECT c.name, COUNT(*) AS orders, SUM(s.quantity) AS units
FROM sales s JOIN customers c ON s.customer_id = c.customer_id
WHERE c.country = 'JPN' AND s.status = 'shipped'
GROUP BY c.name ORDER BY units DESC LIMIT 3;

-- LEFT JOIN anti-join: customers who never bought 20 gizmos in one order
SELECT c.name FROM customers c
LEFT JOIN sales s ON s.customer_id = c.customer_id AND s.product = 'gizmo' AND s.quantity = 20
WHERE s.order_id IS NULL ORDER BY c.name LIMIT 5;

-- Comma joins with the condition in WHERE are hash joins, not cross products
SELECT c.country, COUNT(DISTINCT c.customer_id) AS customers, SUM(s.quantity * s.unit_price) AS revenue
FROM customers c, sales s WHERE s.customer_id = c.customer_id
GROUP BY c.country ORDER BY revenue DESC LIMIT 3;

-- Views. A single-table view stays lazy: filters on the view are pushed into
-- the base table's index together with the view's own WHERE
CREATE VIEW shipped_sales AS
SELECT order_id, customer_id, country, product, quantity FROM sales WHERE status = 'shipped';
SELECT COUNT(*) FROM shipped_sales WHERE country = 'JPN' AND quantity >= 15;
DROP VIEW shipped_sales;

-- Set operations: each branch runs in its own index; the driver combines them
SELECT country FROM customers WHERE customer_id <= 10
UNION
SELECT country FROM sales WHERE quantity = 20 AND product = 'gizmo'
ORDER BY 1;
SELECT customer_id FROM customers
EXCEPT
SELECT customer_id FROM sales WHERE product = 'gizmo' AND quantity = 20
ORDER BY 1 LIMIT 5;

-- information_schema: which columns of sales are indexed?
SELECT column_name, data_type, is_nullable, is_indexed
FROM information_schema.columns WHERE table_name = 'sales' ORDER BY ordinal_position;

-- LIKE; a prefix pattern on an indexed column is an index prefix query
SELECT COUNT(*) FROM sales WHERE product LIKE 'gi%';

-- IN lists, dates
SELECT name, country, signup_date
FROM customers
WHERE country IN ('USA', 'CAN')
ORDER BY name
LIMIT 5;

-- Writes
INSERT INTO customers (customer_id, name, country, signup_date)
VALUES (51, 'Ada', 'GBR', DATE '2026-09-30');
UPDATE sales SET status = 'returned' WHERE order_id = 1;
DELETE FROM customers WHERE customer_id = 51;

-- DDL
CREATE TABLE events (id BIGINT NOT NULL, kind VARCHAR, payload VARCHAR NOINDEX, at TIMESTAMP(3));
DROP TABLE events;
```

**4. Look at the data in Redis**

Each row is a plain HASH, and each table has one RediSearch index:

```bash
docker exec redis-adbc-test redis-cli HGETALL public:sales:42
```

```bash
docker exec redis-adbc-test redis-cli FT.INFO idx:public:sales
```

To see the `FT.AGGREGATE` / `HMGET` commands the driver sends for each query,
run `MONITOR` in a second terminal while you run SQL:

```bash
docker exec -it redis-adbc-test redis-cli MONITOR
```

Stop Redis with `docker compose down`.

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
- **Metadata**, all in one hash slot: `adbc:{meta}:table:<schema>:<table>`
  (column types as JSON), `adbc:{meta}:seq:*` (row ids),
  `adbc:{meta}:tables:<schema>`, `adbc:{meta}:schemas`, and for views
  `adbc:{meta}:view:<schema>:<view>` (the SELECT text and its columns) and
  `adbc:{meta}:views:<schema>`. Tables and views share one namespace.
  Each table's metadata records its row key prefix and index name, and
  `adbc:{meta}:prefixes` / `adbc:{meta}:indexes` reserve them, so a renamed
  table's rows can never be shared with a new table of the old name.
  `adbc:{meta}:cleanup` lists tables with a dropped-column cleanup in progress. Metadata written by
  v0.0.1 (`adbc:meta:*`, `adbc:schemas`, …) is migrated automatically on
  the first connection.

How SQL is executed:

| Query shape | Execution |
|-|-|
| `WHERE __rowid = N` | Direct `HMGET` of the row HASH, index bypassed |
| Filter / sort / limit | `FT.AGGREGATE <idx> "<pushed-down query>" LOAD 1 @__key SORTBY … LIMIT … WITHCURSOR`, then pipelined `HMGET` of only the needed columns |
| `COUNT(*)`, `GROUP BY` + `COUNT/SUM/AVG/MIN/MAX` | `FT.AGGREGATE … APPLY exists(@c) … GROUPBY … REDUCE …` over SORTABLE fields, HASHes never opened |
| `col LIKE 'abc%'` on an indexed string column | TAG prefix query `@c:{abc*}` (other patterns are checked by the driver) |
| `col IN (…)`, `col IN (SELECT …)`, `col = a OR col = b` on an indexed column | Index union query (`(@c:[a a] \| @c:[b b])` or `@c:{a \| b}`) |
| Subqueries | Uncorrelated: run once per statement, results reused. Correlated: run per outer row with the outer values as constants (so they still use the index), memoised |
| `UNION` / `INTERSECT` / `EXCEPT` | Each branch runs as its own query (using its own index); the driver combines, de-duplicates and sorts the results |
| CTEs, derived tables | Run once; the outer query filters, sorts and groups them in memory |
| `ALTER TABLE` | Metadata only (optimistic `WATCH`/`MULTI` on the table's metadata), plus `FT.ALTER` for `ADD COLUMN` and a background `HDEL` pass for `DROP COLUMN` |
| Views | Single-table views without GROUP BY/aggregates/LIMIT are expanded in place: the outer query's filters are rewritten over the base table and run in its index. Other views are computed once per query, like a derived table |
| Joins | Each table's own WHERE/ON filters run in its index (except on the NULL-supplying side of an outer join). Inner joins are reordered to start from the table with the fewest matches (counted by the index). Equality conditions drive a hash join; when the next table's key is indexed and there are ≤ 1,000 distinct keys, only matching rows are fetched with an index union. The joined rows are then grouped/sorted in memory |
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
  `CREATE TABLE [IF NOT EXISTS] t AS SELECT …` (column names and types come
  from the query; every indexable column is indexed),
  `DROP TABLE [IF EXISTS] t`, `CREATE/DROP SCHEMA`,
  `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] v [(cols)] AS SELECT …`,
  `DROP VIEW [IF EXISTS] v`
- `ALTER TABLE [IF EXISTS] t` with one of `RENAME TO u`,
  `RENAME [COLUMN] a TO b`, `ADD [COLUMN] [IF NOT EXISTS] c TYPE [NOINDEX]`,
  `DROP [COLUMN] [IF EXISTS] c`. All of them only change metadata, so they
  take the same time at any table size:
  - `RENAME TO` keeps the table's row keys and index (fixed when the table
    was created), so no row is touched.
  - `RENAME COLUMN` keeps the column's HASH field; only its SQL name changes.
  - `ADD COLUMN` adds the column to the index with `FT.ALTER`; existing rows
    read it as NULL. `NOT NULL` and `DEFAULT` are not supported here.
  - `DROP COLUMN` hides the column immediately and removes its field from
    existing rows in the background (resumed by the next connection if the
    process exits first). A dropped column's values never reappear, even if
    a column with the same name is added later.
- `INSERT INTO t [(cols)] VALUES (…), (…)` with literals or `?` / `$n`
  parameters, and `INSERT INTO t [(cols)] SELECT …`
- `[WITH name [(cols)] AS (SELECT …), …] SELECT … FROM item {, item |
  [INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item ON … | USING (…) | CROSS JOIN item}
  [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT n] [OFFSET m]`,
  where an item is a table, a CTE, or `(SELECT …)`, each with an optional
  alias (`t.col` qualifies a column),
  with `COUNT/SUM/AVG/MIN/MAX`, `CASE` (simple and searched), arithmetic,
  `CAST`, `IS [NOT] NULL`, `[NOT] LIKE` / `ILIKE` (with `ESCAPE`), subqueries (scalar `(SELECT …)`, `EXISTS`,
  `[NOT] IN (SELECT …)`, correlated or not, in SELECT/WHERE/HAVING and in
  `UPDATE`/`DELETE`),
  `BETWEEN`, `IN`, `COALESCE`, `LOWER/UPPER/LENGTH/ABS/CONCAT`, `from_hex`
- `UNION [ALL]`, `INTERSECT [ALL]`, `EXCEPT [ALL]` (`INTERSECT` binds
  tighter; parenthesized branches may have their own `ORDER BY`/`LIMIT`).
  Columns are matched by position and widened to a common type; NULLs count
  as equal when removing duplicates. `ORDER BY` on the combined result uses
  output column names or positions. Set operations work anywhere a query
  does (subqueries, CTEs, views, CTAS, `INSERT … SELECT`)
- `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]`; NULLs sort last by default in
  both directions
- `SELECT` without `FROM` for literal expressions
- `information_schema` (read-only, built from the driver's metadata when
  queried): `schemata`, `tables` (`BASE TABLE` / `VIEW`), `columns`
  (`ordinal_position`, `data_type`, `is_nullable`, `numeric_precision`,
  `numeric_scale`, `datetime_precision`, and `is_indexed`), and `views`
  (`view_definition`). Any SQL works on them, including joins
- `UPDATE t SET … [WHERE …]`, `DELETE FROM t [WHERE …]`
- Not supported: `NATURAL JOIN`, `WITH RECURSIVE`, `LATERAL`, `ANY`/`ALL`
  comparisons, window functions
- Types: `BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE PRECISION,
  NUMERIC(p,s), VARCHAR/TEXT, VARBINARY/BLOB, DATE, TIME(p), TIMESTAMP(p)
  [WITH TIME ZONE]`

Tables can be qualified as `schema.table` or `redis.schema.table`. Schemas
are key namespaces (default `public`). There are no transactions (autocommit
only).

## Options

| Option | Level | Meaning |
|-|-|-|
| `uri` | database | `redis://[user:pass@]host:port/db`, or `rediss://…` for TLS |
| `username`, `password` | database | Credentials (override the URI) |
| `adbc.redis.address`, `adbc.redis.db` | database | Used when no URI is given |
| `adbc.redis.cluster` | database | `auto` (default) / `true` / `false`: OSS Cluster API client or single endpoint |
| `adbc.redis.default_schema` | database | Schema for unqualified names (default `public`) |
| `adbc.redis.aggregate_pushdown` | database, statement | `exact` / `all` / `none` |
| `adbc.redis.ingest.index_columns` | statement | Comma-separated columns to index on bulk ingest (`*` = all indexable) |

Scale tips: keep column names short (they are repeated in every HASH), raise
`hash-max-listpack-entries` / `hash-max-listpack-value` so small rows use the
compact encoding, and index only the columns you filter or aggregate on.

## Building and testing

See the [Quick start](#quick-start) for building. To run the ADBC validation
suite against the local Redis:

```bash
cd go/validation
REDIS_URI=redis://localhost:6379/0 uv run pytest -v tests/
```

The suite accepts any Redis 8.x server, so it can also target a remote or
Redis Cloud database. Use `rediss://` if the database requires TLS, and use
an empty database: the suite leaves its test tables (`test_*`, `getobjects*`,
`statistics*`) behind.

```bash
REDIS_URI='rediss://default:<password>@<host>:<port>/0' uv run pytest -v tests/
```

SQL features beyond the validation suite (CASE, HAVING, CTAS, …) have Go
integration tests with exact expected results. They need a running Redis and
are skipped when `REDIS_URI` is unset (from `go`):

```bash
REDIS_URI=redis://localhost:6379/0 go test -run TestSQL ./...
```

To test against a local 3-shard Redis Cluster (from `go`):

```bash
docker compose --profile cluster up --detach --wait redis-cluster
```

```bash
cd validation && REDIS_URI=redis://localhost:7001/0 uv run pytest -v tests/
```

Using the driver from Python:

```python
import adbc_driver_manager.dbapi as dbapi
conn = dbapi.connect(driver="go/build/libadbc_driver_redis.so",
                     db_kwargs={"uri": "redis://localhost:6379/0"})
cur = conn.cursor()
cur.adbc_ingest("my_table", arrow_table, mode="create")  # any pyarrow.Table
cur.execute("SELECT country, SUM(quantity) FROM sales WHERE quantity > 10 GROUP BY country")
print(cur.fetch_arrow_table())
```

The C FFI shim in `go/pkg` was generated with driverbase-go's `ffitemplate`
(`go run main.go -prefix Redis -driver <path>/go`).
