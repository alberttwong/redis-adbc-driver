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

The full validation suite (324 passed, 0 failed) has been run against:

| Server | Version | Connection |
|-|-|-|
| Redis Open Source (`redis:8.4` Docker image) | 8.4.4, Search 8.4.10 | `redis://` |
| Redis Open Source cluster, 3 shards (OSS Cluster API) | 8.4.4, Search 8.4.10 | `redis://` |
| Redis Cloud, single shard | 8.6.2, Search 8.6.10 | `redis://` and TLS (`rediss://`)*† |
| Redis Cloud Pro, 2 shards, through the proxy endpoint | 8.6.2 | `redis://`† |
| Redis Cloud Pro, 2 shards, OSS Cluster API enabled | 8.6.2 | `redis://`† |

\* Run before cluster support was added; not re-run since.
† Run before the two temporary-table ingest tests were enabled (322
passed, 0 failed); not re-run since.

The remaining 10 skipped tests and 1 expected failure are features the
driver doesn't offer: constraints, statistics, a second catalog,
transactions, and parameter-type introspection.

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
    `SADD/SMEMBERS/SISMEMBER/SREM`, `MULTI/EXEC/WATCH`, `INFO`, `PING`, `HELLO`,
    plus `CLUSTER` on an OSS Cluster API endpoint (to discover the shards)
  - keys: `adbc:*`, plus the row prefix of each schema you use (`public:*`
    by default, and `pg_temp_*` for temporary tables)

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

-- Temporary tables and views belong to this connection: other connections
-- don't see them, and they are dropped when it closes
CREATE TEMP TABLE big_jpn AS
SELECT order_id, customer_id, quantity FROM sales WHERE country = 'JPN' AND quantity >= 18;
SELECT c.name, COUNT(*) AS orders FROM big_jpn b JOIN customers c ON c.customer_id = b.customer_id
GROUP BY c.name ORDER BY orders DESC, c.name LIMIT 3;

-- Set operations: each branch runs in its own index; the driver combines them
SELECT country FROM customers WHERE customer_id <= 10
UNION
SELECT country FROM sales WHERE quantity = 20 AND product = 'gizmo'
ORDER BY 1;
SELECT customer_id FROM customers
EXCEPT
SELECT customer_id FROM sales WHERE product = 'gizmo' AND quantity = 20
ORDER BY 1 LIMIT 5;

-- Date/time functions (UTC): monthly totals for the first quarter
SELECT TO_CHAR(DATE_TRUNC('month', ordered_at), 'YYYY-MM') AS month, COUNT(*) AS orders, SUM(quantity) AS units
FROM sales WHERE EXTRACT(QUARTER FROM ordered_at) = 1
GROUP BY month ORDER BY month;

-- Timestamp arithmetic: the bound is computed once, then runs as an index range
SELECT COUNT(*) AS last_week FROM sales
WHERE ordered_at >= TIMESTAMP '2025-12-31 00:00:00' - INTERVAL '7 days';
SELECT MAX(ordered_at) - MIN(ordered_at) AS span, AGE(MAX(ordered_at), MIN(ordered_at)) AS age FROM sales;

-- Math, string and conditional functions. The index still answers the WHERE
-- clause; functions over columns run in the driver on the matching rows
SELECT INITCAP(product) AS product, ROUND(AVG(unit_price), 2) AS avg_price,
       ROUND(SUM(quantity * unit_price * (1 - COALESCE(discount, 0))), 2) AS net
FROM sales WHERE status = 'shipped' GROUP BY product ORDER BY net DESC;
SELECT LPAD(CAST(customer_id AS VARCHAR), 4, '0') AS id, SPLIT_PART(email, '@', 2) AS domain,
       SUBSTRING(name FROM 10) AS num, IIF(country IN ('USA', 'CAN', 'MEX'), 'americas', 'other') AS region
FROM customers WHERE customer_id <= 3 ORDER BY customer_id;

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

To see the `FT.AGGREGATE` / `FT.CURSOR` commands the driver sends for each query,
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
  finds matching rows     │   computes GROUPBY/REDUCE without
  (filter, sort, limit),  │   opening the HASHes
  LOAD reads their fields ▼
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
- **Temporary tables and views** use the same layout in a private schema
  per connection, `pg_temp_<id>` (rows `pg_temp_<id>:<table>:<rowid>`, index
  `idx:pg_temp_<id>:<table>`), which is never added to
  `adbc:{meta}:schemas`. The connection id comes from the counter
  `adbc:{meta}:temp:next` when the connection creates its first temporary
  object. `adbc:{meta}:temp:owners` lists the connections that have a
  temporary schema, and `adbc:{meta}:temp:alive:<id>` shows that the owner
  is still open: it has a 2-minute TTL that the connection refreshes every
  30 seconds. Closing the connection drops its temporary objects and these
  keys. A new connection drops the temporary objects of any owner whose
  alive key has expired, re-checking the key before each object.

How SQL is executed:

| Query shape | Execution |
|-|-|
| `WHERE __rowid = N` | Direct `HMGET` of the row HASH, index bypassed |
| Filter / sort / limit | `FT.AGGREGATE <idx> "<pushed-down query>" [SORTBY …] [LIMIT …] LOAD … WITHCURSOR COUNT 10000`: the rows come back in the cursor pages, up to 10,000 at a time (fewer for wide tables) |
| `COUNT(*)`, `GROUP BY` + `COUNT/SUM/AVG/MIN/MAX` | `FT.AGGREGATE … APPLY exists(@c) … GROUPBY … REDUCE …` over SORTABLE fields, HASHes never opened |
| `col LIKE 'abc%'` on an indexed string column | TAG prefix query `@c:{abc*}` (other patterns are checked by the driver) |
| `col IN (…)`, `col IN (SELECT …)`, `col = a OR col = b` on an indexed column | Index union query (`(@c:[a a] \| @c:[b b])` or `@c:{a \| b}`) |
| Subqueries | Uncorrelated: run once per statement, results reused. Correlated: run per outer row with the outer values as constants (so they still use the index), memoised |
| `UNION` / `INTERSECT` / `EXCEPT` | Each branch runs as its own query (using its own index); the driver combines, de-duplicates and sorts the results |
| CTEs, derived tables | Run once; the outer query filters, sorts and groups them in memory |
| `CREATE TEMP TABLE` / `VIEW` | Same as a permanent table or view, in the connection's `pg_temp_<id>` schema. Unqualified names are looked up there first (in memory, no extra round trip) |
| `TRUNCATE` | `FT.DROPINDEX … DD` (deletes every row the index knows about, as `DROP TABLE` does), then `FT.CREATE` with the same key prefix and index name. Not isolated from concurrent writes to the same table |
| `ALTER TABLE` | Metadata only (optimistic `WATCH`/`MULTI` on the table's metadata), plus `FT.ALTER` for `ADD COLUMN` and a background `HDEL` pass for `DROP COLUMN` |
| Views | Single-table views without GROUP BY/aggregates/LIMIT are expanded in place: the outer query's filters are rewritten over the base table and run in its index. Other views are computed once per query, like a derived table |
| Joins | Each table's own WHERE/ON filters run in its index (except on the NULL-supplying side of an outer join). Inner joins are reordered to start from the table with the fewest matches (counted by the index). Equality conditions drive a hash join; when the next table's key is indexed and there are ≤ 1,000 distinct keys, only matching rows are fetched with an index union. The joined rows are then grouped/sorted in memory |
| `UPDATE … FROM`, `DELETE … USING`, `MERGE` | The target is joined with the other items as above: its own filters (in WHERE, or in MERGE's ON) run in its index, and an equality on an indexed target column is an index lookup join, also through a no-op cast like dbt's `s.id::text = t.id::text`. `MERGE` is source `LEFT JOIN` target, or `FULL JOIN` with `WHEN NOT MATCHED BY SOURCE` clauses (which need every target row); `ON FALSE` reads the target only for those. Changes are then written by row key with pipelined `HSET`/`HDEL`/`DEL`, and new rows like `INSERT` does |
| Anything the index can't answer exactly | Evaluated by the driver on rows fetched from the HASHes |

Pushed down into the index: numeric range/equality predicates on indexed
columns (`@c:[lo hi]`), string equality on indexed columns (`@c:{value}`),
`ORDER BY` on indexed columns, `LIMIT/OFFSET`, and aggregates.

Row values always come from the HASHes as stored, never from the index sort
vectors: `LOAD @c` on a SORTABLE numeric attribute returns the sort vector's
double printed with 12 significant digits, which would round 64-bit integers,
timestamps, decimals and doubles. So the driver names the columns it needs
(`LOAD n @__key @__rowid @c …`) only when each one is a string, a 16/32-bit
integer, boolean, date or time, or not indexed, and otherwise uses `LOAD *`,
which returns the HASH fields unchanged. Sorts run on aliases
(`LOAD @c AS __sort0 SORTBY @__sort0`) so that on a cluster the coordinator
merges the shards' results on the numeric sort values rather than on the
strings `LOAD *` returns.

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
  `DROP TABLE [IF EXISTS] t [CASCADE | RESTRICT]`,
  `CREATE SCHEMA [IF NOT EXISTS] s`,
  `CREATE [OR REPLACE] VIEW [IF NOT EXISTS] v [(cols)] AS SELECT …`,
  `DROP VIEW [IF EXISTS] v [CASCADE | RESTRICT]`,
  `ALTER VIEW [IF EXISTS] v RENAME TO w` (`ALTER TABLE v RENAME TO w` also
  renames a view, as in Postgres; other `ALTER TABLE` actions on a view are
  rejected). Renaming changes metadata only; views that read the object by
  its old name stop working, as they do after a table rename
- `CREATE {TEMP | TEMPORARY} TABLE [IF NOT EXISTS] t (…)`,
  `CREATE TEMP TABLE [IF NOT EXISTS] t AS [(]SELECT …[)]`,
  `CREATE [OR REPLACE] TEMP VIEW [IF NOT EXISTS] v [(cols)] AS SELECT …`, and
  bulk ingest with `adbc.ingest.temporary`. Temporary tables and views work
  like permanent ones in every statement (joins with permanent tables, CTEs,
  `ALTER`, `TRUNCATE`, `DROP`, …), with these differences:
  - **Scope:** only the ADBC connection that created them sees them, so two
    connections can each have a temporary table with the same name.
  - **Lifetime:** they are dropped when the connection is closed
    (`AdbcConnectionRelease`). If the process exits without closing it, the
    next connection to open after the 2-minute heartbeat TTL has expired
    drops them. A connection that can't reach Redis for longer than that may
    lose its temporary objects.
  - **Names:** an unqualified name means a temporary table or view first, so
    it shadows a permanent one of the same name, as in Postgres. `schema.t`
    always means a permanent table, and `pg_temp.t` the temporary one
    (`CREATE TABLE pg_temp.t` creates a temporary table). Without `TEMP`,
    `CREATE TABLE` / `CREATE VIEW` create permanent objects, and bulk ingest
    without `adbc.ingest.temporary` targets the permanent table. The schema
    names `pg_temp` and `pg_temp_<digits>` are reserved.
  - **Metadata:** `GetObjects` and `information_schema` list them under the
    schema `pg_temp`, only for the connection that owns them and only while
    it has any. `information_schema.tables` reports temporary tables as
    `LOCAL TEMPORARY`, as Postgres does.
  - **Views:** a temporary view may read temporary and permanent tables. Its
    unqualified names resolve like the connection's own statements:
    temporary objects first, then the schema that was current when the view
    was created. A permanent view can't refer to a temporary table or view
    (`CREATE VIEW` fails; use `CREATE TEMP VIEW`), and temporary objects
    never shadow the names inside a permanent view. A view keeps only its
    SQL text, so a temporary table created later with the name of a
    permanent table that a temporary view reads takes that table's place in
    the view.
- `DROP SCHEMA [IF EXISTS] s [CASCADE | RESTRICT]`: `RESTRICT` (the default)
  refuses a schema that still has tables or views; `CASCADE` drops its views,
  then its tables (indexes and rows), then the schema. Dependencies between
  tables and views aren't tracked, so `CASCADE` / `RESTRICT` on `DROP TABLE`,
  `DROP VIEW` and `DROP COLUMN` are accepted but don't drop or check views
  that read the object (such a view fails when queried, as it does after a
  plain `DROP`)
- `TRUNCATE [TABLE] [ONLY] t [, …] [RESTART IDENTITY | CONTINUE IDENTITY]
  [CASCADE | RESTRICT]` removes every row and keeps the table (metadata, key
  prefix, index). Row ids continue unless `RESTART IDENTITY` is given. All
  names are checked before any table is emptied
- `ALTER TABLE [IF EXISTS] t` with one of `RENAME TO u`,
  `RENAME [COLUMN] a TO b`, `ADD [COLUMN] [IF NOT EXISTS] c TYPE [NOINDEX]`,
  `DROP [COLUMN] [IF EXISTS] c [CASCADE | RESTRICT]`. All of them only change metadata, so they
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
  parameters, and `INSERT INTO t [(cols)] SELECT …` (the query may be
  parenthesized: `INSERT INTO t (SELECT …)`)
- `[WITH name [(cols)] AS (SELECT …), …] SELECT … FROM item {, item |
  [INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item ON … | USING (…) | CROSS JOIN item}
  [WHERE …] [GROUP BY …] [HAVING …] [ORDER BY …] [LIMIT n] [OFFSET m]`,
  where an item is a table, a CTE, or `(SELECT …)`, each with an optional
  alias (`t.col` qualifies a column),
  with `COUNT/SUM/AVG/MIN/MAX`, `CASE` (simple and searched), arithmetic,
  `CAST(x AS type)` / `x::type`, `IS [NOT] NULL`, `[NOT] LIKE` / `ILIKE` (with `ESCAPE`), subqueries (scalar `(SELECT …)`, `EXISTS`,
  `[NOT] IN (SELECT …)`, correlated or not, in SELECT/WHERE/HAVING and in
  `UPDATE`/`DELETE`/`MERGE`),
  `BETWEEN`, `IN`, `COALESCE`, `LOWER/UPPER/LENGTH/ABS`, `CONCAT(a, …)` and
  `CONCAT_WS(sep, a, …)` (NULL arguments are skipped, as in Postgres; `||`
  returns NULL if either side is NULL), `from_hex`
- `UNION [ALL]`, `INTERSECT [ALL]`, `EXCEPT [ALL]` (`INTERSECT` binds
  tighter; parenthesized branches may have their own `ORDER BY`/`LIMIT`).
  Columns are matched by position and widened to a common type; NULLs count
  as equal when removing duplicates. `ORDER BY` on the combined result uses
  output column names or positions. Set operations work anywhere a query
  does (subqueries, CTEs, views, CTAS, `INSERT … SELECT`)
- `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]`; NULLs sort last by default in
  both directions
- Math functions: `ROUND(x [, n])` and `TRUNC(x [, n])` (`n` may be
  negative: `ROUND(1250, -2)` is 1300), `FLOOR`, `CEIL` / `CEILING`, `MOD`,
  `POWER` / `POW`, `SQRT`, `LN`, `LOG(x)` (base 10) / `LOG(b, x)`, `LOG10`,
  `EXP`, `SIGN`, `ABS`, `RANDOM()`
  - `ROUND` rounds half away from zero: exactly on `NUMERIC`, and on
    `DOUBLE PRECISION` too, by the value as written (`ROUND(2.675e0, 2)` is
    2.68). Postgres rounds doubles half to even
  - Integers and doubles keep their type. `ROUND(NUMERIC(p,s), n)` has scale
    `n` (at most `s`); `ROUND(x)`, `TRUNC(x)`, `FLOOR` and `CEIL` scale 0.
    `MOD` has its arguments' common type; `SQRT`, `LN`, `LOG`, `EXP`,
    `POWER` and `RANDOM` return `DOUBLE PRECISION`
  - Errors as in Postgres for the square root of a negative number, the
    logarithm of zero or of a negative number, `MOD` by zero, zero to a
    negative power, and overflow
- String functions; positions are 1-based and count characters, not bytes:
  - `SUBSTRING(s, start [, len])`, `SUBSTRING(s FROM start [FOR len])`,
    `SUBSTR`, `LEFT(s, n)` / `RIGHT(s, n)` (a negative `n` drops characters
    from the other end), `POSITION(sub IN s)`, `STRPOS(s, sub)`
  - `TRIM([BOTH | LEADING | TRAILING] [chars] FROM s)`, `TRIM(s [, chars])`,
    `LTRIM`, `RTRIM`, `BTRIM` (spaces by default), `LPAD` / `RPAD(s, len
    [, fill])`, `REPLACE`, `REVERSE`, `REPEAT`, `INITCAP`, `MD5`,
    `STARTS_WITH`, `SPLIT_PART(s, delim, n)` (a negative `n` counts from
    the end)
  - `REGEXP_REPLACE(s, pattern, replacement [, flags])` replaces the first
    match, or all of them with flag `g`; flags `i`, `n`, `p`, `w`, `q`
    (literal pattern) work as in Postgres, and `\1` … `\9` and `\&` in the
    replacement insert groups and the match. Patterns use Go's RE2 syntax,
    so backreferences and lookaround are not available
- Conditional functions: `NULLIF(a, b)`, `GREATEST(…)` / `LEAST(…)` (NULL
  arguments are ignored), and `IIF(cond, a, b)` (like `CASE`, only the chosen
  branch is evaluated). `COALESCE`, `GREATEST` and `LEAST` widen their
  arguments to a common type (`COALESCE(int_col, 2.5)` is `NUMERIC`); a
  string literal, or a number that type holds exactly, takes the other
  arguments' type (`COALESCE(int_col, 0)` stays `INTEGER`,
  `GREATEST(d, '2024-01-01')` is a `DATE`)
- Functions return NULL for a NULL argument, except `COALESCE`, `NULLIF`,
  `GREATEST`, `LEAST` and `IIF`. Functions of constants are computed once,
  so `WHERE n > ROUND(?)` is still an index query; functions over columns are
  evaluated by the driver on the rows the index returns. `RANDOM()` is
  computed for every row and never pushed down
- Date/time functions, all in UTC (the current time is fixed once per
  statement):
  - `CURRENT_DATE`, `CURRENT_TIMESTAMP` / `NOW()`, `CURRENT_TIME`,
    `LOCALTIMESTAMP`, `LOCALTIME`
  - `EXTRACT(field FROM x)` / `DATE_PART('field', x)` for `year`, `isoyear`,
    `quarter`, `month`, `week` (ISO), `day`, `dow` (0 = Sunday), `isodow`,
    `doy`, `hour`, `minute`, `second`, `milliseconds`, `microseconds`, `epoch`,
    `decade`, `century`, `millennium`; shortcuts `YEAR()`, `QUARTER()`,
    `MONTH()`, `WEEK()`, `DAY()`, `DAYOFYEAR()`, `HOUR()`, `MINUTE()`,
    `SECOND()`
  - `DATE_TRUNC('unit', x)` (dates stay dates), `DATE_DIFF('unit', a, b)`
    (unit boundaries crossed), `LAST_DAY(d)`
  - `MAKE_DATE`, `MAKE_TIME`, `MAKE_TIMESTAMP`, `MAKE_TIMESTAMPTZ`,
    `TO_TIMESTAMP(epoch_seconds)`, `EPOCH(x)`, `EPOCH_MS(x)`
  - `TO_CHAR(x, format)`, `TO_DATE(text, format)`,
    `TO_TIMESTAMP(text, format)` with Postgres patterns (`YYYY`, `MM`,
    `Mon`/`Month`, `DD`, `Day`/`DY`, `HH24`/`HH12`, `MI`, `SS`, `MS`, `US`,
    `AM`/`PM`, `Q`, `IW`, `"text"`, `FM`)
- Intervals and date/time arithmetic:
  - `INTERVAL '1 year 2 months 3 days 04:05:06'`, `'1.5 hours'`, `'2 days ago'`,
    `'3 04:05:06'`, `'1-2'` (years-months), ISO 8601 `'P1Y2M3DT4H'`,
    `INTERVAL '2' HOUR`, `INTERVAL 7 DAY` / `INTERVAL ? DAY`, and
    `CAST('7 days' AS INTERVAL)`
  - `timestamp ± interval`, `date ± interval` (gives a timestamp),
    `date ± integer` (days), `date - date` (days), `timestamp - timestamp`
    (an interval of days and time), `time ± interval`, `time - time`,
    `interval ± interval`, `interval * n`, `interval / n`, `-interval`
  - Months follow the calendar and clamp to the end of the month
    (`TIMESTAMP '2024-01-31' + INTERVAL '1 month'` is Feb 29). Comparisons
    treat a month as 30 days, so `INTERVAL '1 day' = INTERVAL '24 hours'`
  - `AGE(a, b)` / `AGE(x)` (years, months, days and time), and `EXTRACT`
    on intervals
  - Intervals are returned as Arrow `month_day_nano_interval`; Arrow
    interval and duration values can be bound and ingested
- `SELECT` without `FROM` for literal expressions
- `information_schema` (read-only, built from the driver's metadata when
  queried): `schemata`, `tables` (`BASE TABLE` / `VIEW` / `LOCAL TEMPORARY`),
  `columns` (`ordinal_position`, `data_type`, `is_nullable`, `numeric_precision`,
  `numeric_scale`, `datetime_precision`, and `is_indexed`), and `views`
  (`view_definition`). Any SQL works on them, including joins
- `[WITH …] UPDATE t [[AS] a] SET col = …, … [FROM item, …] [WHERE …]` and
  `[WITH …] DELETE FROM t [[AS] a] [USING item, …] [WHERE …]` (the Postgres
  forms). The FROM / USING items are written like a SELECT's FROM clause
  (tables, views, CTEs and `(SELECT …)`, with commas or joins) and are
  joined with the target; SET and WHERE can read all of them. A target row
  that matches several FROM / USING rows is updated or deleted once. For
  UPDATE the matches must give the same new values, otherwise it's an error
  (Postgres silently uses one of them)
- `[WITH …] MERGE INTO t [[AS] a] USING item [[AS] s] ON cond` followed by
  any number of
  - `WHEN MATCHED [AND cond] THEN UPDATE SET … | DELETE | DO NOTHING`
  - `WHEN NOT MATCHED [BY TARGET] [AND cond] THEN INSERT [(cols)] VALUES (…)
    | INSERT DEFAULT VALUES | DO NOTHING`
  - `WHEN NOT MATCHED BY SOURCE [AND cond] THEN UPDATE SET … | DELETE | DO NOTHING`

  The source is a table, view, CTE or `(SELECT …)`. Each source row with
  its matching target row, each unmatched source row and (with `BY SOURCE`
  clauses) each unmatched target row takes the first clause of its kind
  whose condition holds, as in Postgres. `WHEN NOT MATCHED` clauses see only
  the source's columns (so `VALUES (id, name)` reads the source), `BY
  SOURCE` clauses only the target's. Changing a target row for more than one
  source row is an error ("MERGE command cannot affect row a second time");
  matching more than one is fine if only one match changes it. NULL keys
  never match. The result is the number of rows inserted, updated and
  deleted. `INSERT DEFAULT VALUES` inserts NULLs (column defaults aren't
  stored)
- Not supported: `NATURAL JOIN`, `WITH RECURSIVE`, `LATERAL`, `ANY`/`ALL`
  comparisons, window functions
- Types: `BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE PRECISION,
  NUMERIC(p,s), VARCHAR/TEXT, VARBINARY/BLOB, DATE, TIME(p), TIMESTAMP(p)
  [WITH TIME ZONE], INTERVAL` (interval columns are stored but not indexed)

Tables can be qualified as `schema.table` or `redis.schema.table`, and
`pg_temp.table` is the connection's temporary table. Schemas are key
namespaces (default `public`). There are no transactions (autocommit
only). `UPDATE`, `DELETE` and `MERGE` find their rows and compute and check
every change first (new values and casts, `NOT NULL`, MERGE's
one-change-per-row rule), so such an error leaves the table untouched. Then
they write, in pipelined batches of up to 1,000 rows (`MERGE`: updates, then
deletes, then inserts). If a write fails part-way, for example because the
connection drops, the batches already written stay written, and other
clients can see a partly applied statement.

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

## Performance

Measured with `go/examples/bench.py` (see below) on the sample data set
scaled to **100,000 `sales` rows × 10 columns** and 50 `customers`.
Setup: Apple M4 (10 cores, 16 GB), Docker Desktop, Redis 8.4.4 (Search
8.4.10) in Docker on the same machine. Times are the median of 5 runs after a
warm-up, measured in Python and including the Arrow transfer; "Redis
commands" are counted with `INFO commandstats` on the standalone server (a
cluster adds the shard fan-out, `_FT.AGGREGATE` / `_FT.CURSOR`). Numbers on a
laptop vary from run to run; treat them as orders of magnitude.

Bulk ingest (`adbc_ingest`): 100,000 rows in 1.6 s standalone (62,000 rows/s)
and 0.56 s on a 3-shard cluster (180,000 rows/s).

| Query | Rows out | Standalone (ms) | 3-shard cluster (ms) | Redis commands |
|-|-:|-:|-:|-:|
| Point lookup, `WHERE __rowid = N` | 1 | 1.7 | 1.0 | 2 |
| `COUNT(*)` with `ts >= TIMESTAMP … - INTERVAL '30 days'` | 1 | 1.4 | 1.0 | 2 |
| Indexed TAG filters + `ORDER BY … LIMIT 10` | 10 | 4.4 | 1.9 | 2 |
| `COUNT(*)` with an indexed range | 1 | 5.0 | 2.5 | 2 |
| `GROUP BY` with `COUNT` / integer `SUM` (index reduce) | 6 | 6.9 | 3.1 | 2 |
| Selective join (index lookup join) + `GROUP BY` | 8 | 25 | 27 | 6 |
| `COUNT(*)` on a view, filter pushed into its base table | 1 | 27 | 15 | 4 |
| Correlated scalar subquery for each of 50 customers | 50 | 31 | 28 | 54 |
| `UNION` of two indexed filters | 1,750 | 48 | 46 | 6 |
| Fetch 10% of the rows (indexed range) | 10,000 | 50 | 68 | 4 |
| `COUNT(*)` with `LIKE 'gi%'` (index prefix query) | 1 | 62 | 64 | 4 |
| Filter on a non-indexed column (`notes LIKE '%x%'`) | 1 | 272 | 279 | 12 |
| Unfiltered join + `GROUP BY` (hash join over all rows) | 6 | 345 | 358 | 16 |
| `GROUP BY` with `AVG` of a `DOUBLE` (driver-side) | 6 | 463 | 522 | 12 |
| `GROUP BY` of an expression `SUM(quantity * unit_price)` | 5 | 494 | 549 | 12 |
| `GROUP BY DATE_TRUNC('month', ts)` | 12 | 499 | 542 | 12 |
| Full scan, `SELECT *` | 100,000 | 679 | 825 | 13 |

What drives the numbers:

- **Queries the index answers on its own take milliseconds**, independent of
  table size: counts and ranges, TAG equality filters with `ORDER BY … LIMIT`,
  and `GROUP BY` with `COUNT` and integer `SUM`/`MIN`/`MAX`.
- **Reading rows costs about 3–7 µs per row.** Rows come back inside the
  `FT.AGGREGATE` cursor pages, up to 10,000 per page, so reading all 100,000
  rows takes a dozen commands and 0.3–0.8 s. Queries that need the driver to
  see every row pay this: aggregates the index can't compute exactly (`AVG`
  of doubles, expressions, `DATE_TRUNC` groups), filters on non-indexed
  columns, and unfiltered joins. Of the full scan's 0.68 s, Redis itself
  takes about 0.35 s to produce the rows (measured from inside the
  container); the rest is decoding, Arrow conversion, and Docker Desktop's
  port forwarding (about 15 ms per page). Up to v0.0.3 the driver fetched
  each row with its own pipelined `HMGET` instead, at about 30 µs per row
  (3.3–3.8 s for the full scan).
- **Selective joins are fast** because the smaller side is filtered in its
  index first and only matching rows of the other side are fetched. A
  correlated subquery costs one indexed query per distinct outer value
  (about 0.6 ms each here).

Known ways to make the slow cases faster (not done yet):

- `COUNT(*)` with a `LIKE` prefix, and `COUNT(*)` on a lazy view, still read
  the matched rows to re-check them, although the index has already matched
  them exactly; the index could answer them alone.

To reproduce (from `go`, with a Redis running):

```bash
REDIS_URI=redis://localhost:6379/0 uv run --project validation --with redis python examples/bench.py --rows 100000
```

The script **replaces** the `sales` and `customers` tables with benchmark data;
run `examples/load_sample.py` afterwards to get the quick-start data back.

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
