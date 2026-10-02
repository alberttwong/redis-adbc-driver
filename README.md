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
| Redis Open Source (`redis:8.6.2` Docker image) | 8.6.2, Search 8.6.0 | `redis://` |
| Redis Open Source cluster, 3 shards (OSS Cluster API) | 8.6.2, Search 8.6.0 | `redis://` |
| Redis Cloud, single shard | 8.6.2, Search 8.6.10 | `redis://` and TLS (`rediss://`)*† |
| Redis Cloud Pro, 2 shards, through the proxy endpoint | 8.6.2 | `redis://`† |
| Redis Cloud Pro, 2 shards, OSS Cluster API enabled | 8.6.2 | `redis://`† |

Local testing uses the Redis version that Redis Cloud runs (currently
8.6.2), so `go/compose.yaml` pins that image.

\* Run before cluster support was added; not re-run since.
† Run before the two temporary-table ingest tests were enabled (322
passed, 0 failed); not re-run since.

The remaining 10 skipped tests and 1 expected failure are features the
driver doesn't offer: constraints, statistics, a second catalog,
transactions, and parameter-type introspection.

Redis Flex (RAM + SSD) databases are not supported yet, and the driver
refuses them when it connects; see
[Server requirements](#server-requirements).

## Server requirements

You don't create any indexes or enable any settings yourself. The one hard
requirement is the Query Engine.

**What you need**

- **Redis with the Query Engine (RediSearch).** It is built into Redis 8.x
  (e.g. the `redis:8.6.2` image). On older versions use Redis Stack, or Redis
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
- **ACL permissions,** if the database has ACL users: the commands and keys
  under **ACL users** below.

**What the driver does for you**

- `CREATE TABLE` and bulk ingest create the table's index
  (`FT.CREATE idx:<schema>:<table> …`) and its metadata keys. `DROP TABLE`
  removes the index and all of the table's rows (a statement still writing
  to the table then deletes what it writes; see "Key prefixes" in the
  architecture section).
- Every filterable column is indexed by default. Narrow this with `NOINDEX`
  in `CREATE TABLE` or `adbc.redis.ingest.index_columns` on ingest. Queries
  still work on columns that aren't indexed: the driver scans the table's
  rows and filters them itself, which is slower on large tables but correct.
- Every query passes `TIMEOUT 0`, so the server's default search timeout
  (which can return partial results) doesn't apply. You don't need to change
  any `search-*` settings or enable keyspace notifications. The client's own
  timeouts still apply; see **Timeouts** below.

**Timeouts**

The driver's client waits up to 5 minutes for each reply from Redis, and
to send each command, on standalone servers and clusters alike. Some
commands take longer as the data grows: an `FT.AGGREGATE` page that sorts or
groups a big table, `FT.DROPINDEX … DD` of a big table, a pipeline of 1,000
rows. And a busy or briefly blocked server (a fork for a snapshot, a
failover, a slow cluster shard) delays every reply. When a reply takes
longer than the timeout, the statement fails with an error that names the
setting:

```text
[redis] FT.AGGREGATE failed: read tcp 127.0.0.1:57705->127.0.0.1:7103: i/o timeout (Redis did not reply within the read timeout, 5m0s: raise adbc.redis.read_timeout, or set it to 0 for no timeout)
```

| Setting | Default | What it limits |
|-|-|-|
| `adbc.redis.read_timeout` (database or connection option), or the URI's `read_timeout` | `5m` | waiting for each reply |
| `adbc.redis.write_timeout` (database or connection option), or the URI's `write_timeout` | the read timeout | sending each command |
| the URI's `dial_timeout` | `5s` | connecting |

- **Values** are durations such as `30s`, `10m` or `1h`, or a number of
  seconds. `0` means no timeout. The URI's parameters are parsed by go-redis
  (`redis://host:6379/0?read_timeout=30m`), where `0` and `-1` also mean no
  timeout.
- **Precedence:** the options override the URI's parameters. A connection's
  option overrides the database's, for the statements that the connection
  runs after it is set (the connection gets a new client). Connecting uses
  the database's.
- **Retries:** go-redis sends most commands again after a reply timed out
  or the connection broke (on a standalone server up to 3 times), so a
  statement can wait a few times the read timeout before it fails. The
  driver turns that off for the commands where a second run isn't safe:
  search commands (a repeated `FT.CURSOR READ` would return the next page,
  and the rows of the lost one would be missing), and `SET … NX` and `SADD`
  where the driver acts on the reply. Transactions (`MULTI` … `EXEC`) aren't
  sent again once they were sent.
- **Other URI parameters** that go-redis parses (`pool_size`, `max_retries`,
  …) apply to the single-endpoint client only. The cluster client takes the
  credentials, TLS and the three timeouts from the URI.
- **Earlier versions** (v0.0.7 and before) used go-redis's defaults: 5
  seconds, in effect 10 seconds on a standalone server.

**ACL users**

If the database has ACL users, the driver's user needs these commands. The
lists were checked with users that have exactly these rules, running the Go
integration tests and the ADBC validation suite on Redis 8.6.2, standalone
and a 3-shard cluster:

```text
# Every connection: queries, DDL, DML, views, bulk ingest, temporary tables
+ping +info +ft._list +ft.create +ft.alter +ft.info +ft.dropindex +ft.aggregate +ft.cursor +ft.search +ft.profile +get +set +del +exists +incrby +hset +hget +hmget +hgetall +hdel +hincrby +hexists +sadd +srem +smembers +sismember +multi +exec +watch +unwatch
# A cluster (OSS Cluster API) also: the client reads the slot map and the commands' key positions
+cluster|slots +command
# adbc.redis.rename_rekey also
+incr +dump +restore
```

- **Keys:** `~adbc:*` (the driver's metadata), `~<schema>:*` for each schema
  the user works in (`~public:*` by default), and `~pg_temp_*` for temporary
  tables. Temporary tables and bulk ingest need no other commands.
- **What they're for:** `PING` and `INFO` to connect (`INFO` also tells a
  cluster apart, and gives the server version); the `FT.*` commands for the
  tables' indexes and queries (`FT.PROFILE` checks that a `LIKE 'abc%'`
  filter's prefix expansion was complete: without it such filters are
  still right, but are evaluated by the driver; `FT.INFO` lists an index's
  attributes for `ALTER TABLE … ADD COLUMN`); the string, hash and set
  commands for metadata and rows (`HGETALL` reads pending re-keying
  renames on every connect, and `HEXISTS` checks that a new table's key
  prefix was never released); and `MULTI` / `EXEC` / `WATCH` / `UNWATCH`
  for metadata transactions.
- **Not needed:** `HELLO` and `AUTH` are always allowed. Add `+select` if the
  URI names a database other than 0 (not tested).
- **On a cluster,** ACL users are per node: create the user on every node.
- **For example,** for dbt on a cluster, with `rename_rekey`:

  ```text
  ACL SETUSER dbt on >password resetchannels ~adbc:* ~public:* ~pg_temp_* +ping +info +ft._list +ft.create +ft.alter +ft.info +ft.dropindex +ft.aggregate +ft.cursor +ft.search +ft.profile +get +set +del +exists +incrby +hset +hget +hmget +hgetall +hdel +hincrby +hexists +sadd +srem +smembers +sismember +multi +exec +watch +unwatch +cluster|slots +command +incr +dump +restore
  ```

A **read-only user** needs these commands, and read access to the keys:

```text
+ping +info +ft._list +ft.aggregate +ft.cursor +ft.search +ft.profile +get +exists +hget +hmget +hgetall +smembers +sismember
```

```text
ACL SETUSER reader on >password resetchannels %R~adbc:* %R~public:* +ping +info +ft._list +ft.aggregate +ft.cursor +ft.search +ft.profile +get +exists +hget +hmget +hgetall +smembers +sismember
```

(On a cluster, also `+cluster|slots +command`.)

- **What it can do:** queries, views, `information_schema`, and the ADBC
  metadata calls (`GetObjects`, `GetTableSchema`): what dbt's tests and
  `dbt docs generate` run (this was tested with the driver, not with dbt).
- **What it can't:** every change is refused before anything changes, with
  Redis's reason, for example `failed to allocate row ids: NOPERM User reader
  has no permissions to run the 'multi' command` for an `INSERT`. That
  includes temporary tables (allocating a temporary schema needs `INCRBY`),
  so dbt unit tests and snapshots need a user that can write.
- **Connecting needs no writes,** with two exceptions. The first connection
  to a new database records the table registry, so a read-only user can't
  be the first to connect (`failed to build the table registry: NOPERM …`).
  And a connection that finds a re-keying rename or a temporary schema that
  another connection abandoned cleans it up: a read-only user can't, and
  leaves that to the next connection that can write. (Redis's `ACL LOG`
  then shows the refused commands.)

**Key patterns don't cover every search command.** RediSearch commands that
change indexes declare no keys, so key patterns don't apply to them:

- **`FT.DROPINDEX … DD`** deletes every row of a table, even for a user that
  may only read the table's keys. `FT.CREATE`, `FT.ALTER` and the
  `FT.ALIAS*` commands can change what the driver's indexes cover. So a user
  that mustn't change data must not get `FT.CREATE`, `FT.ALTER`,
  `FT.DROPINDEX` or `FT.ALIAS*` (the read-only rules above leave them out).
- **Queries** do check keys in Redis 8.6: `FT.SEARCH` and `FT.AGGREGATE` on
  an index whose key prefix the user can't read fail with `NOPERM User does
  not have the required permissions to query the index`, so per-schema key
  patterns keep a user out of other schemas' tables. `FT._LIST` still lists
  every index, so such a user sees the other schemas' table names. Check
  this on other Redis versions.

**Errors:** a command the user may not run fails with Redis's `NOPERM`
error. One that Redis refuses inside a transaction is reported with the
command and its key, rather than as `EXECABORT Transaction discarded because
of previous errors`: `failed to create view: NOPERM No permissions to access
a key ('set' on adbc:{meta}:view:public:v, in a MULTI transaction that Redis
discarded)`.

**Limitation: only tables created through the driver are visible**

A table exists for the driver only if its metadata key
(`adbc:{meta}:table:<schema>:<table>`) and its index (`idx:<schema>:<table>`) exist.
HASHes and indexes created some other way (for example your own
`row:*` / `idx:rows` layout) won't appear as tables. To query existing data,
load it through the driver; bulk ingest from Arrow is the quickest route.

**Limitation: Redis Flex databases are not supported yet**

[Redis Flex](https://redis.io/docs/latest/operate/rs/flex/) (formerly Auto
Tiering) keeps most of a database on SSD, for datasets of a terabyte and
more. Search on Flex can't index tables the way the driver needs. Every
table's index has a `NUMERIC SORTABLE` row-ID field, and every query reads
rows through `FT.AGGREGATE` cursors. So when the driver connects to a Flex
database it fails with `Redis Flex databases are not supported`, before
writing anything.

| Flex database | Search on Flex | Missing for the driver |
|-|-|-|
| Redis Cloud Essentials | Not available | Search itself (the driver reports that search is missing) |
| Redis Cloud Pro | Opt-in Preview | `FT.AGGREGATE`; `NUMERIC` fields (per Redis's docs; not tested here) |
| Redis Software 8.2.0-78, Redis 8.6.2, Search 8.6.11 | Included (`search_on_bigstore`) | `FT.AGGREGATE`, `FT.ALTER`, `FT.DROPINDEX … DD`; `NUMERIC`, `GEO` and `SORTABLE` fields. `FT.SEARCH` returns only keys, and indexes need `SKIPINITIALSCAN` (tested, see [Building and testing](#building-and-testing)) |

Redis's announcement of Search on Flex says Redis Software already offers
`NUMERIC` fields and `FT.AGGREGATE` on Flex, and that Redis Cloud will follow.
But the latest Docker image (8.2.0-78, built 2026-09-01) rejects both. The
connect check tests whether `FT.AGGREGATE` works, not whether the database
is Flex. So a Flex release that supports it is no longer refused.

## Quick start

Requirements: Go 1.26+, a C toolchain (cgo), Docker, and
[uv](https://docs.astral.sh/uv/). Run everything from the `go` directory.

Each [GitHub release](https://github.com/alberttwong/redis-adbc-driver/releases)
also has prebuilt libraries for macOS (Apple Silicon) and Linux (x86-64 and
arm64). The Linux libraries run on every Linux that
[Redis Software supports](https://redis.io/docs/latest/operate/rs/references/supported-platforms/):
RHEL 8 and 9 (and compatible distributions), Ubuntu 20.04 and later, and
Amazon Linux 2023. To use one instead of building, put it at the path
`make build` would create (`go/build/libadbc_driver_redis.dylib` or `.so`).

**1. Start Redis 8.6 and build the driver**

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

-- EXISTS / IN correlated only by equalities can run once instead, as a hash
-- semi-join (here the matching customer ids also filter sales in the index)
SELECT COUNT(*) FROM sales s
WHERE EXISTS (SELECT 1 FROM customers c WHERE c.customer_id = s.customer_id AND c.country = 'JPN');

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

-- Window functions run in the driver, after WHERE / GROUP BY / HAVING.
-- Each customer's latest order (the dbt de-duplication idiom), and the same
-- with QUALIFY: the earliest customer per country
SELECT customer_id, order_id, ordered_at FROM (
  SELECT customer_id, order_id, ordered_at,
         ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ordered_at DESC) AS rn
  FROM sales
) AS latest WHERE rn = 1 ORDER BY customer_id LIMIT 5;
SELECT name, country, signup_date FROM customers
QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date) = 1 ORDER BY country;

-- Ranks and running totals over grouped rows; LAG and a moving average
SELECT country, SUM(quantity) AS units, RANK() OVER (ORDER BY SUM(quantity) DESC) AS rnk,
       SUM(SUM(quantity)) OVER (ORDER BY SUM(quantity) DESC) AS running_units
FROM sales GROUP BY country ORDER BY rnk;
SELECT order_id, quantity, LAG(quantity) OVER w AS prev,
       AVG(quantity) OVER (w ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg
FROM sales WHERE customer_id = 7 WINDOW w AS (ORDER BY order_id) ORDER BY order_id LIMIT 5;

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

-- Regular expressions (RE2 syntax) and SIMILAR TO run in the driver, on the
-- rows the rest of the WHERE clause finds in the index
SELECT name, REGEXP_SUBSTR(email, '([0-9]+)@', 1, 1, '', 1) AS num, SUBSTRING(email FROM '@(.*)$') AS domain
FROM customers WHERE email ~ '^customer[0-9]@' ORDER BY customer_id LIMIT 3;
SELECT product, COUNT(*) AS orders FROM sales
WHERE country = 'JPN' AND product SIMILAR TO 'g(adget|izmo)' GROUP BY product ORDER BY product;

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
COMMENT ON COLUMN events.payload IS 'Raw JSON, as received';
DROP TABLE events;
```

**4. Look at the data in Redis**

Each row is a plain HASH, and each table has one RediSearch index. A renamed
table keeps its key names unless `adbc.redis.rename_rekey` is set (see
`ALTER TABLE` below), and a table created again after `DROP TABLE` gets new
ones (`public:sales~2:`, see "Key prefixes" below);
`information_schema.tables` shows each table's `key_prefix` and
`index_name`:

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
  (NULL = field absent, except for missing values, below; a hidden `__rowid`
  field keeps all-NULL rows alive). A table that isn't the first of its
  name has the prefix `<schema>:<table>~N:` and the index
  `idx:<schema>:<table>~N` (see "Key prefixes" below).
- **Selective index**: `FT.CREATE idx:<schema>:<table> ON HASH PREFIX 1
  <schema>:<table>:` covering only filterable columns: numeric, boolean,
  decimal, date/time and timestamp columns as `NUMERIC SORTABLE`, strings as
  `TAG SEPARATOR "\x1f" CASESENSITIVE INDEXEMPTY SORTABLE UNF`. Binary columns and columns
  declared `NOINDEX` are stored in the HASH only.
- **Strings in the index**: a TAG doesn't hold every string as it is.
  RediSearch cuts a value at its first NUL byte, splits it at the separator,
  trims ASCII whitespace (space, `\t`, `\n`, `\v`, `\f`, `\r`) from both
  ends of each piece and cuts each piece to 4,096 bytes: so `'ab '`, `' ab'`
  and `'ab'` all have the tag `ab`, and `' '` has the tag `''` (Unicode
  spaces are kept). The sort vector holds the whole value, except that it
  also stops at a NUL byte. So each indexed string column's metadata
  records whether the index holds all its values exactly, or some only as
  other tags (`tag_values: normalized`), or some cut at a NUL byte
  (`truncated`). Every write (`INSERT`, `UPDATE`, `MERGE`, bulk ingest,
  `ADD COLUMN … DEFAULT`) raises it before writing the rows, and it never
  goes down (not even on `DELETE` or `TRUNCATE`). Then:
  - On a column held exactly, `col = 'v'` is the exact TAG query `@c:{v}`,
    so `COUNT`, aggregates and `LIMIT` still run in the index.
  - Otherwise, and for a constant that isn't held exactly, the query looks
    up the constant's tag (`@c:{ab}` for `'ab '`) and the driver re-checks
    the rows, so the index no longer counts, aggregates or limits on its
    own for that predicate. `IN` lists, index lookup joins and `LIKE`
    prefixes look tags up the same way (and are always re-checked).
  - A truncated column is also sorted, grouped and read (`LOAD *`) by the
    driver. A constant with an ASCII control character other than
    whitespace can't be written in a TAG query, so it is checked by the
    driver.
  - Tables created by earlier versions (no `tags_checked` in their
    columns' metadata) count as truncated until a background check,
    started by the first statement that reads one on a connection, has read
    every row (about 0.4 s for 100,000 rows of 10 columns) and recorded
    what they hold. It records nothing if the table's metadata changed
    meanwhile (a writer raising a level), and starts over; a connection
    whose check failed tries again a minute later.
  - A statement that read a table's metadata before another connection
    raised a level may see that connection's new rows as the index holds
    them (count a new `'ab '` as `'ab'`, or sort or read a new value with a
    NUL byte up to that byte), in the same window in which it may or may
    not see them at all. Earlier versions of the driver don't raise levels
    when they write.
- **Metadata**, all in one hash slot: `adbc:{meta}:table:<schema>:<table>`
  (column types, defaults, missing values, string levels, comments and
  `CHECK` constraints as JSON), `adbc:{meta}:seq:*` (row ids),
  `adbc:{meta}:tables:<schema>`, `adbc:{meta}:schemas`, and for views
  `adbc:{meta}:view:<schema>:<view>` (the SELECT text, its columns and comments) and
  `adbc:{meta}:views:<schema>`. Tables and views share one namespace.
  Each table's metadata records its row key prefix and index name, and
  `adbc:{meta}:prefixes` / `adbc:{meta}:indexes` reserve them, so a renamed
  table's rows can never be shared with a new table of the old name.
  `adbc:{meta}:names:next` holds the last `~N` handed out for each table
  name (see "Key prefixes" below).
  `adbc:{meta}:cleanup` lists tables with a dropped-column cleanup in progress.
  `adbc:{meta}:rekey` records renames that are moving a table's rows
  (`adbc.redis.rename_rekey`), keyed by the old prefix, and
  `adbc:{meta}:rekey:alive:<id>` (30-second TTL, renewed by the renaming
  connection) shows that the connection doing one is alive. A record taken
  over from a connection that went away is marked `recovering`.
  `adbc:{meta}:released` counts how often each key prefix was released (by
  `DROP TABLE` or a re-keying rename), one field per prefix that was ever
  released; a statement checks it and `adbc:{meta}:prefixes` to tell when
  its table's rows have moved away. Metadata written by
  v0.0.1 (`adbc:meta:*`, `adbc:schemas`, …) is migrated automatically on
  the first connection.
- **Key prefixes are never reused.** A table takes the first of
  `<schema>:<table>:`, `<schema>:<table>~2:`, … that no table has had
  before, with the index name of the same N (`idx:<schema>:<table>~2`).
  So a table created again after `DROP TABLE` gets new keys, and so does a
  re-keying rename onto a name that a table had before. The reason is
  writes still running when a table is dropped: a statement that read the
  table's metadata before the `DROP` goes on writing rows under the old
  prefix until it finds out, and those rows outlive the `DROP`, which only
  deletes the rows its index knows about. Up to v0.0.7 the next table of
  that name took the same prefix, and its row ids started at 1 again, so
  its rows were written into those leftover HASHes: a column that should
  have been NULL read the dropped table's value. Now:
  - An insert reads the table's metadata again in the `MULTI` that
    allocates its row ids (each Arrow batch, in bulk ingest), at no extra
    cost. If the table was dropped, renamed or created again since the
    statement started, it writes nothing.
  - A write of more than 1,000 rows (`CREATE TABLE … AS`, `INSERT …
    SELECT`, bulk ingest, a large `UPDATE`) also checks that the table
    still has its keys: between pipelines of 1,000 rows once 100 ms have
    passed since the last check, and always after its last pipeline (one
    round trip each). Smaller writes check at the end only when 100 ms have
    passed since they read the metadata, as before.
  - A statement whose table was dropped (or whose rows a re-keying rename
    moved away) stops and fails: "table … was renamed with its rows moved to
    new keys, or dropped, while this statement was writing to it; some of
    its changes may be lost or have gone to another table". If no table has
    the old prefix any more, it first deletes every key it wrote there
    (one pipelined `DEL` per 1,000 keys).
  - Keys can still be left under a dropped table's prefix, where no table or
    query reads them: when the writing process exits before it finds out,
    and when a write of 1,000 rows or fewer that read the metadata within
    100 ms of the `DROP` reaches Redis after it (it doesn't check, and
    succeeds, as if it had run before the `DROP`). To remove them, check
    that no table has the prefix (`information_schema.tables.key_prefix`,
    or `SISMEMBER adbc:{meta}:prefixes '<prefix>'` returns 0), then delete
    its keys, on every primary of a cluster: `redis-cli --scan --pattern
    '<prefix>*' | xargs redis-cli unlink`.
  - **Upgrading:** tables created by earlier versions keep their names and
    work as before. Prefixes released by v0.0.6 and v0.0.7 are recorded in
    `adbc:{meta}:released` and are skipped too, so rows they left behind
    can't reach a new table; v0.0.5 and earlier didn't record releases.
    Upgrade every client that writes to the database: an earlier version
    still gives a dropped table's prefix to the next table of that name.
- **Missing values**: `ALTER TABLE … ADD COLUMN c … DEFAULT v` doesn't
  rewrite the existing rows. As with Postgres's "missing value", the
  column's metadata records `v` and the row id high-water mark (the
  `adbc:{meta}:seq:*` value, read in the transaction that adds the column),
  and a row with `__rowid` up to the mark and no `c` field reads `v`. Rows
  inserted later have higher ids, so for them an absent field is NULL as
  usual. To keep that true:
  - A statement that read the metadata before the `ALTER` allocates its row
    ids in one `MULTI` with a read of the metadata, sees the new column, and
    writes `v` for it.
  - Setting an old row's `c` to NULL (`UPDATE`, `MERGE`) also writes a
    hidden marker field, `__null_<field>`, which setting a value removes.
  - The index has no entry for an old row's absent field, so a filter on `c`
    that `v` satisfies is widened to `(@c:[…] | @__rowid:[-inf mark])` and
    re-checked by the driver (one that `v` doesn't satisfy stays an exact
    index query), and `ORDER BY` and aggregates on `c` run in the driver.
  - `TRUNCATE` drops the missing values with the rows.
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
| Queries that can't return rows: `WHERE false`, `LIMIT 0`, … | Their rows aren't read. A query with `LIMIT 0` (or `FETCH FIRST 0 ROWS ONLY`, or a `HAVING` or `QUALIFY` that is never true) returns without a command. One whose `WHERE` is never true doesn't read its FROM items, and the rest of it runs over no rows: `COUNT(*)` without `GROUP BY` still returns one row, 0. A condition is never true when it is FALSE or NULL whatever the rows hold: its parts that read no column, parameter or subquery are evaluated once, and `AND` is never true if one side is, `OR` if both are (`false`, `1 = 0`, `NULL`, `x > 0 AND false`, `NOT true`, …). This applies at every level (derived tables, CTEs, views, join items, `UNION` branches, subqueries) and to `INSERT … SELECT`, `UPDATE`, `DELETE`, `MERGE` and `CREATE TABLE … AS`. The derived tables, CTEs and views such a query reads are planned but not run, so its columns and their types are those of the full query, and unknown tables, columns and functions and wrong argument counts are still errors; `ExecuteSchema` and `CREATE VIEW` plan their query the same way. This is how dbt asks for a query's columns (`select * from (…) as __dbt_sbq where false limit 0`, for model contracts, snapshots and unit tests), and what `dbt run --empty` reads (`(select * from t where false limit 0)`) |
| Filter / sort / limit | `FT.AGGREGATE <idx> "<pushed-down query>" [SORTBY …] [LIMIT …] LOAD … WITHCURSOR COUNT 10000`: the rows come back in the cursor pages, up to 10,000 at a time (fewer for wide tables) |
| `COUNT(*)`, `GROUP BY` + `COUNT/SUM/AVG/MIN/MAX`, `BOOL_OR/BOOL_AND/EVERY` of a boolean column | `FT.AGGREGATE … APPLY exists(@c) … GROUPBY … REDUCE …` over SORTABLE fields, HASHes never opened (`BOOL_OR` / `BOOL_AND` are `MAX` / `MIN` of the stored 0 and 1). On a cluster, a shard whose rows of a group all lack the value sends NaN as its partial `SUM`, which made the group's `SUM` and `AVG` NaN up to v0.0.7 (`AVG` of an integer column also in the default mode). So when a `SUM` comes back NaN for a group that has values, the driver runs the command again with `case(exists(@c), @c, 0)` in place of `@c` (about 2.5 times as long; a real NaN, from adding +Infinity and -Infinity, stays NaN), and a server without `case()` aggregates the query in the driver |
| Other aggregates (`STRING_AGG`, `STDDEV`, percentiles, `ANY_VALUE`, …), `DISTINCT` and `FILTER (WHERE …)` | Reduced by the driver over the rows fetched from the HASHes (the index still filters them). An aggregate's `FILTER` is evaluated before its arguments, so `SUM(1 / x) FILTER (WHERE x <> 0)` never divides by zero. Ordered-set aggregates keep each group's values and sort them once |
| `GROUP BY ROLLUP` / `CUBE` / `GROUPING SETS` | Each grouping set runs as its own grouped query: an `FT.AGGREGATE … GROUPBY` when a plain `GROUP BY` of its columns would be one, otherwise in the driver, where the sets share one read of the rows. The driver combines the groups of all sets (`UNION ALL`, NULL for the columns a set doesn't group), then applies HAVING, window functions, QUALIFY, DISTINCT, ORDER BY and LIMIT to the combined rows |
| `col LIKE 'abc%'` on an indexed string column | TAG prefix query `@c:{abc*}` on the prefix's tag, re-checked by the driver (other patterns are checked by the driver alone). RediSearch expands a prefix into at most `search-max-prefix-expansions` tags (200 by default, per shard) and silently ignores the rest, so the driver first runs the prefix query under `FT.PROFILE … LIMIT 0 1` (one more round trip, about 0.3 ms) and, unless every shard's profile reports no warning, checks the prefix on every row the rest of the WHERE clause selects instead. The server's configuration is never changed |
| `col IN (…)`, `col IN (SELECT …)`, `col = a OR col = b` on an indexed column | Index union query (`(@c:[a a] \| @c:[b b])` or `@c:{a \| b}`), for up to 1,000 values. Above that (or on an unindexed column) the driver checks each row against a hash set of the values, built once per statement: always for `IN (SELECT …)`, and for a literal list of 16 or more constants when the column's type can't fail to compare with them (otherwise value by value, as `=` would) |
| Subqueries | Uncorrelated: run once per statement, results reused. Correlated `[NOT] EXISTS` / `[NOT] IN` over one table, CTE or derived table, whose only references to the outer query are `inner = outer` conditions in its WHERE: a hash semi-join (anti-join for `NOT`), which runs the subquery once without those conditions. A small inner side (≤ 1,000 rows, counted by the index) is read first, and then `EXISTS` / `IN` also filter the outer table in its index (nothing matches if the subquery has no rows; a union of ≤ 1,000 keys on an indexed outer column); a larger one is read only once running per outer row has cost about as much. Other correlated subqueries: run per outer row with the outer values as constants (so they still use the index), memoised |
| `(a, b) [NOT] IN (SELECT …)` (and `= ANY`, `<> ALL`) | The subquery runs once, and each row is checked against a hash set of its rows, built once per statement. Telling NULL from false (for `NOT IN`) costs one lookup per pattern of NULLs among the subquery's rows. If the subquery has no row without a NULL, `IN` reads no row. For each indexed column among the items with ≤ 1,000 distinct values in the subquery's rows, an index union fetches only the rows with those values. Correlated, with only `inner = outer` conditions: the hash semi-join below, with a set of rows per key. A literal list, `(a, b) IN ((1, 'x'), (2, 'y'))`, is an `OR` of `AND`s; each indexed column that every row of the list sets gets an index union |
| `x op ANY / ALL (SELECT …)` | `= ANY` is `IN` and `<> ALL` is `NOT IN`, with the same index unions, hash sets and semi-joins. Other operators: an uncorrelated subquery runs once, and `<`, `<=`, `>`, `>=` compare with its smallest or largest value (when all the values are of one type class); otherwise each row is compared with every value |
| `UNION` / `INTERSECT` / `EXCEPT` | Each branch runs as its own query (using its own index); the driver combines, de-duplicates and sorts the results |
| CTEs, derived tables | Run once, unless the query that reads them can't return rows (see above); the outer query filters, sorts and groups them in memory |
| `WITH RECURSIVE` | Working-table iteration in memory: the non-recursive term runs once, then the recursive term runs against the rows the last iteration added (its joins with tables still use their indexes, such as an index lookup join on the new rows' keys) until it adds none. `UNION` drops rows produced before. At most 10,000 iterations and 1,000,000 rows. With `SEARCH` / `CYCLE`, the recursive term runs once per row of the working table, to know each row's parent |
| `LATERAL`, `GENERATE_SERIES` | Join items computed while the join runs. One that reads earlier FROM items runs for each row joined so far (a `LATERAL` subquery memoised on the values it reads, like a correlated subquery); otherwise it runs once and is joined like a derived table. A join with such an item keeps its written order. `GENERATE_SERIES` makes its rows in memory, at most 1,000,000 per call |
| `SELECT DISTINCT` | Over plain expressions it is the same as `GROUP BY` them, so it runs in the index (`FT.AGGREGATE … GROUPBY`) when they're indexed columns. Otherwise (stars, aggregates, window functions, `DISTINCT ON`) the driver removes duplicate rows after the rest of the query, NULLs counting as equal. `LIMIT` / `OFFSET` apply afterwards and never run in the index. As in Postgres, `ORDER BY` must use the select list, and `DISTINCT ON` keys must match the leading `ORDER BY` expressions |
| Window functions | Computed by the driver once the rows are known: after WHERE, GROUP BY and HAVING (which still run in the index when they can), before QUALIFY, ORDER BY and LIMIT. Rows are hashed into partitions and each partition is sorted once per distinct PARTITION BY / ORDER BY; frame aggregates add and remove rows as the frame slides (O(1) amortized per row; sums, averages and variances add and subtract exact sums, also of doubles). With `EXCLUDE CURRENT ROW / GROUP / TIES` the frame has a hole, so each row's frame is aggregated afresh (O(frame) per row), as PostgreSQL does; `FIRST_VALUE` / `LAST_VALUE` / `NTH_VALUE` stay O(1), also with `IGNORE NULLS`. A query with window functions never pushes its LIMIT into the index |
| `CREATE TEMP TABLE` / `VIEW` | Same as a permanent table or view, in the connection's `pg_temp_<id>` schema. Unqualified names are looked up there first (in memory, no extra round trip) |
| `TRUNCATE` | `FT.DROPINDEX … DD` (deletes every row the index knows about, as `DROP TABLE` does), then `FT.CREATE` with the same key prefix and index name. Not isolated from concurrent writes to the same table |
| `ALTER TABLE` | Metadata only (optimistic `WATCH`/`MULTI` on the table's metadata, one for all the actions of a statement), plus `FT.INFO` and one `FT.ALTER` for the added columns and a background `HDEL` pass for `DROP COLUMN`. `ADD COLUMN … DEFAULT` records a missing value instead of writing the rows. Adding a `CHECK` (`ADD COLUMN … CHECK`, `ADD CONSTRAINT … CHECK`) first reads the columns it uses from every row, with one `FT.AGGREGATE` cursor scan for all the statement's new `CHECK`s, to check them. With `adbc.redis.rename_rekey`, `RENAME TO` also creates the new name's index, copies every row the old index lists with pipelined `DUMP` / `RESTORE … REPLACE` (a cursor page at a time; one key per command, so it works on a cluster), switches the metadata in one transaction, then runs `FT.DROPINDEX <old> DD` |
| Views | `CREATE VIEW` plans the body without running it, which checks its tables, columns and function calls. Single-table views without GROUP BY/aggregates/window functions/LIMIT are expanded in place: the outer query's filters are rewritten over the base table and run in its index. Other views are computed once per query, like a derived table |
| Joins | Each table's own WHERE/ON filters run in its index (except on the NULL-supplying side of an outer join). Inner joins are reordered to start from the table with the fewest matches (counted by the index). Equality conditions drive a hash join; when the next table's key is indexed and there are ≤ 1,000 distinct keys, only matching rows are fetched with an index union. The joined rows are then grouped/sorted in memory. `NATURAL JOIN` is an equality join on the common columns, like `USING`. An item whose `ON` can never be true (`ON false`), or that has no rows, isn't read when that settles the result: an inner join is then empty and reads no item, a `LEFT JOIN` keeps the left rows with NULLs for the item, and a `RIGHT JOIN` returns the item's rows without reading the items before it |
| `UPDATE … FROM`, `DELETE … USING`, `MERGE` | The target is joined with the other items as above: its own filters (in WHERE, or in MERGE's ON) run in its index, and an equality on an indexed target column is an index lookup join, also through a no-op cast like dbt's `s.id::text = t.id::text`. `MERGE` is source `LEFT JOIN` target, or `FULL JOIN` with `WHEN NOT MATCHED BY SOURCE` clauses (which need every target row); `ON FALSE` reads the target only for those. Changes are then written by row key with pipelined `HSET`/`HDEL`/`DEL`, and new rows like `INSERT` does |
| Anything the index can't answer exactly | Evaluated by the driver on rows fetched from the HASHes |

Pushed down into the index: numeric range/equality predicates on indexed
columns (`@c:[lo hi]`), string equality on indexed columns (`@c:{value}`,
re-checked by the driver once the column holds a value the index trims,
splits or cuts; see "Strings in the index" above), `ORDER BY` on indexed
columns, `LIMIT/OFFSET`, and aggregates. A constant
that doesn't fit the column's type exactly (`int_col > 1.5`, `numeric_col =
1.249`, `date_col < TIMESTAMP '… 12:00:00'`) is pushed as an inclusive bound
at its rounded value and re-checked by the driver.

Row values always come from the HASHes as stored, never from the index sort
vectors: `LOAD @c` on a SORTABLE numeric attribute returns the sort vector's
double printed with 12 significant digits, which would round 64-bit integers,
timestamps, decimals and doubles. So the driver names the columns it needs
(`LOAD n @__key @__rowid @c …`) only when each one is a string (unless the
column may hold a NUL byte, at which the sort vector stops), a 16/32-bit
integer, boolean, date or time, or not indexed, and otherwise uses `LOAD *`,
which returns the HASH fields unchanged. Sorts run on aliases
(`LOAD @c AS __sort0 SORTBY @__sort0`) so that on a cluster the coordinator
merges the shards' results on the numeric sort values rather than on the
strings `LOAD *` returns.

Aggregate pushdown (`adbc.redis.aggregate_pushdown`):

- `exact` (default): pushes COUNT, plus SUM/AVG/MIN/MAX over 16/32-bit
  integer, boolean and date columns, which RediSearch returns exactly, and
  BOOL_OR/BOOL_AND/EVERY over boolean columns. Other aggregates are computed
  by the driver.
- `all`: also pushes floating-point, decimal and 64-bit aggregates.
  RediSearch adds doubles in its own order (on a cluster, each shard's
  rows, then the shards' sums) and reports the result with 12 significant
  digits; `AVG` is that sum divided by the count. That is the exact sum
  when it has at most 12 significant digits, such as amounts with two
  decimals, and RediSearch's rounding errors stay below the 12th digit;
  otherwise it differs from it after the 12th digit (`SUM` of 1/3 and 1/7
  is 0.47619047619, not 0.47619047619047616), and sums that cancel can lose
  their small values (1e16, 1, -1e16, 3, 1e16, 0.5, -1e16 came back as 4,
  not 4.5).
- `none`: always aggregates in the driver.

The other modes compute `SUM` and `AVG` of doubles exactly (see
Aggregates, below), so their results don't depend on the mode, the order of
the rows, or the server being standalone or a cluster.

In every mode, `DISTINCT`, `FILTER (WHERE …)` and the other aggregates run
in the driver. RediSearch has reducers that look like some of them, but none
gives SQL's answer: `STDDEV` loses precision on values far from zero (150
values near 10⁹ came back wrong in the 8th significant digit, on a cluster
too) and is 0 instead of NULL for a single value, `QUANTILE` is an
approximate streaming estimate, `TOLIST` de-duplicates in no particular
order, and `FIRST_VALUE` returns NULL when the group's first row lacks the
field, even if later rows have it.

## Supported SQL

- `CREATE TABLE [IF NOT EXISTS] t (col TYPE [column_constraint …] [NOINDEX] [COMMENT 'text'], …
  [, table_constraint, …]) [COMMENT [=] 'text']`, where a column constraint
  is `[CONSTRAINT name] {NOT NULL | NULL | CHECK (expr) | DEFAULT expr |
  PRIMARY KEY | UNIQUE | REFERENCES …}` and a table constraint is
  `[CONSTRAINT name] {CHECK (expr) | PRIMARY KEY (cols) | UNIQUE (cols) |
  FOREIGN KEY (cols) REFERENCES …}` (see Constraints below; the `COMMENT`
  clauses, MySQL / Snowflake style, set the same comments as `COMMENT ON`),
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
- `ALTER TABLE [IF EXISTS] t` with `RENAME TO u`, with
  `RENAME [COLUMN] a TO b`, or with one or more of these, separated by commas:
  `ADD [COLUMN] [IF NOT EXISTS] c TYPE [column_constraint …] [NOINDEX] [COMMENT 'text']`,
  `DROP [COLUMN] [IF EXISTS] c [CASCADE | RESTRICT]`,
  `ALTER [COLUMN] c {SET DEFAULT expr | DROP DEFAULT}`,
  `ADD table_constraint`,
  `DROP CONSTRAINT [IF EXISTS] name [CASCADE | RESTRICT]`. All of them only change metadata, so they
  take the same time at any table size (except `RENAME TO` with
  `adbc.redis.rename_rekey`, and adding a `CHECK`, which reads every row
  first):
  - **Several actions** (dbt's `on_schema_change` and snapshots send
    `alter table t add column "a" …, add column "b" …, drop column "c"`) are
    one change of the table's metadata, made in one `WATCH`/`MULTI`, so
    other statements see the table before all of them or after all of
    them. They apply in the order written, each seeing what the ones before
    it did: `ADD COLUMN x …, ADD CONSTRAINT c CHECK (x > 0)` checks the rows
    with `x`'s default, `DROP COLUMN a, ADD COLUMN a …` gives a new, empty
    column, and two unnamed `CHECK`s on `v` are named `t_v_check` and
    `t_v_check1`. Postgres instead runs all the `DROP`s first, then the
    `ADD COLUMN`s, then the constraints and defaults, so a few orders it
    accepts are errors here, such as a `CHECK` on a column added later in
    the list (write the column first), and `ADD COLUMN a …, DROP COLUMN a`,
    an error there, adds nothing here. A table can't be left without
    columns, but `DROP COLUMN a, ADD COLUMN b …` works on a table whose only
    column is `a`.
  - **All or nothing:** if any action fails (a column that exists or
    doesn't, a `DEFAULT` that doesn't convert, a `CHECK` that some row
    fails, …), none is applied: the metadata, the search index and the
    rows stay as they were. Every definition is checked first, then the
    rows against the new `CHECK`s (one scan for all of them), then the new
    indexed columns are added to the index (one `FT.ALTER`, which adds all
    of them or none), and then the metadata is written. No row is
    rewritten before that: the only row writes are the removal of dropped
    columns' fields afterwards, in the background, which is recorded in
    the metadata and resumed by the next connection if it stops part-way
    (the table is already correct then: the fields are never read or
    reused). If writing the metadata fails (a concurrent change, "try
    again", or a lost connection), the index keeps attributes that no
    column uses, which is harmless, and a later `ADD COLUMN` doesn't reuse
    their names.
  - **`RENAME TO` and `RENAME COLUMN`** take no list, as in Postgres:
    `ALTER TABLE t RENAME TO u, ADD …` is `syntax error at or near ","`,
    and `ALTER TABLE t ADD …, RENAME …` is `syntax error at or near
    "RENAME"`. On a view, only `RENAME TO` is accepted.
  - `RENAME TO` keeps the table's row keys and index (fixed when the table
    was created), so no row is touched. A table that dbt builds as
    `t__dbt_tmp` and renames to `t` keeps the keys `public:t__dbt_tmp:<rowid>`
    and the index `idx:public:t__dbt_tmp` (or `t__dbt_tmp~N`: names are never
    reused, so every build after the first takes a new N).
  - With the option `adbc.redis.rename_rekey` set to `true` (on the
    database, e.g. in dbt's `db_kwargs`, or on the connection), `RENAME TO`
    also moves the rows and the index to the new name's: `public:t:<rowid>`
    and `idx:public:t`, or `~N` if a table has had those before. In dbt's
    swap the previous build's `t` gives them up on every build, so from the
    second build on the keys are `public:t~N:`, with a new N each time. This
    reads and writes every row, so it takes time proportional to the number
    of rows (and about 100 ms more), and the table takes twice its memory
    until the copy is done (if Redis runs out, the rename fails and is
    rolled back). Every rename does it, including dbt's renames to
    `__dbt_backup`. The rows are copied with `DUMP` / `RESTORE`, every field
    included, then the metadata is switched in one transaction and the old
    index is dropped with `DD`, which deletes the old rows. Views are still
    renamed in the metadata only. While the rows move:
    - Readers see the table, complete, under its old name until the switch
      and under the new one after it. A statement that read the table's
      metadata during the move and was still running at the switch, or one
      that ran for 100 ms or more while the rows moved away, fails ("try
      again") rather than returning rows that are gone or now belong to
      another table.
    - Statements that would change the table are refused ("is being
      renamed"): `INSERT`, `UPDATE`, `DELETE`, `MERGE`, `TRUNCATE`, bulk
      ingest that appends, and `ALTER TABLE`. An `ALTER TABLE … ADD`, `DROP`
      or `RENAME COLUMN` that had already started is taken into account when
      the metadata is switched. `DROP TABLE` (and so bulk ingest in
      `replace` mode) is not refused: if it reaches the table before the
      switch, the rename fails.
    - A write statement that read the table's metadata before the move
      started may still be running, so the copy waits 100 ms first. A write
      statement that took longer than that, from reading the metadata to its
      last write, checks afterwards whether the table was being moved (or
      was moved, or dropped), and if so fails with "some of its changes may
      be lost or have gone to another table": like a write that fails
      part-way, some of them may have been applied. Faster statements don't
      pay for the check, unless they write more than 1,000 rows (see "Key
      prefixes" in the architecture section).
    - If the renaming process exits, the table is unchanged under its old
      name and refuses changes until the move's 30-second lease has
      expired. Until then the refusal says to try again when the rename
      has finished, and that a rename whose connection has gone away is
      rolled back once its lease has expired. Then the move is rolled back
      (its copies, its index and the names it reserved are removed), before
      anything else uses the table:
      - **A connection that opens** rolls it back before it is open, even
        one that is closed straight away, as dbt's metadata connections
        are. Connections that open while another one rolls it back wait
        for that (up to 30 seconds).
      - **A connection that was already open** rolls it back at its next
        statement that would change the table, or its next re-keying
        rename, and that statement then goes on.
      - **If the rollback fails** (for example, the user lacks
        `FT.DROPINDEX`), the connection gives it up at once, and the
        statement fails with the reason and says that the next connection
        tries again.

      If it had already switched the metadata, the rename stands, and the
      same connections remove the old rows instead. A renaming connection
      that can't renew its lease for 15 seconds stops before its next step
      and fails; another connection then rolls the move back.
  - `RENAME COLUMN` keeps the column's HASH field; only its SQL name changes.
  - `ADD COLUMN` adds the column to the index with `FT.ALTER`; existing rows
    read it as NULL, or as its `DEFAULT`, which is computed once (so
    `DEFAULT CURRENT_TIMESTAMP` gives them all the time of the `ALTER`) and
    recorded as the column's missing value rather than written to them (see
    the architecture section). A volatile default (`RANDOM()`) is not
    supported here, and `NOT NULL` needs a non-NULL `DEFAULT`.
  - `DROP COLUMN` hides the column immediately and removes its field from
    existing rows in the background (resumed by the next connection if the
    process exits first). A dropped column's values never reappear, even if
    a column with the same name is added later.
  - `ALTER COLUMN … SET DEFAULT` / `DROP DEFAULT` only change what rows
    inserted later get; existing rows keep their values.
  - `ADD COLUMN … CHECK (…)` and `ADD [CONSTRAINT name] CHECK (…)` check
    every existing row first (see Constraints below). `DROP CONSTRAINT`
    removes a `CHECK`; the other constraints aren't kept, so it can't name
    them.
- `COMMENT ON {TABLE | VIEW | COLUMN} name IS 'text' | NULL`, as in
  Postgres (dbt's `persist_docs` runs it). A column is named `t.col`,
  `schema.t.col` or `redis.schema.t.col`, and names resolve as elsewhere
  (temporary objects first). `NULL` or `''` removes the comment.
  - **Storage:** the comment is part of the table's or view's metadata
    (changed with `WATCH`/`MULTI`), so it stays with the object through
    `RENAME TO` and `RENAME COLUMN`, and is dropped with the object, or with
    its column by `DROP COLUMN`. `TRUNCATE` keeps it. With
    `adbc.redis.rename_rekey` the comments move with the metadata, and
    `COMMENT ON` isn't refused while the rows move. `CREATE OR REPLACE VIEW`
    keeps the view's comment and those of the columns that keep their names,
    as Postgres does. `CREATE TABLE … AS` and `CREATE VIEW` copy no comments
    from what they read.
  - **Reading them back:** `information_schema.tables.comment` and
    `information_schema.columns.comment`; `GetObjects`' `remarks` for
    columns (the `GetObjects` schema has no field for a table's comment);
    and `GetTableSchema`, as the field metadata `ARROW:FLIGHT:SQL:REMARKS`
    (the key Flight SQL uses for column remarks) of each column that has
    one. Postgres's `obj_description` / `col_description` aren't supported:
    they take OIDs, which the driver doesn't have.
  - **Errors** are Postgres's: `"v" is not a table` (`COMMENT ON TABLE` on a
    view), `"t" is not a view`, `relation "t" does not exist`, `column "c"
    of relation "t" does not exist`.
- `INSERT INTO t [(cols)] VALUES (…), (…)` with literals, `?` / `$n`
  parameters or `DEFAULT`, `INSERT INTO t DEFAULT VALUES`, and
  `INSERT INTO t [(cols)] SELECT …` (the query may be parenthesized:
  `INSERT INTO t (SELECT …)`)
- Column defaults (`DEFAULT expr`), as in Postgres:
  - A row gets a column's default when it is inserted without a value for
    it: a column left out of the column list of `INSERT … VALUES`,
    `INSERT … SELECT` or MERGE's `INSERT`, `DEFAULT` in place of a value,
    `INSERT … DEFAULT VALUES`, and columns missing from the Arrow data of a
    bulk ingest. An explicit NULL stays NULL (and fails on a `NOT NULL`
    column). `UPDATE … SET col = DEFAULT` is not supported.
  - The default is checked when it is defined: it may not read columns or
    use subqueries, parameters, aggregates, `GROUPING` or window functions,
    its function calls are checked as in a query (`DEFAULT nosuchfunc(NULL)`
    is `function nosuchfunc(unknown) does not exist`), and it must convert
    to the column's type and fit its length (`INTEGER DEFAULT 'abc'` and
    `VARCHAR(3) DEFAULT 'abcd'` are errors then).
    `DEFAULT NULL` is the same as no default.
  - It is computed once per statement (or bulk ingest), so all the rows of
    a statement get the same `CURRENT_TIMESTAMP` / `NOW()` /
    `CURRENT_DATE`, the statement's time. A volatile default (`RANDOM()`)
    is computed for every row.
  - `information_schema.columns.column_default` and `GetObjects`'
    `xdbc_column_def` show the default as written. `CREATE TABLE … AS`
    copies values, not defaults
- String lengths, as in Postgres:
  - **Types:** `VARCHAR(n)` (also `CHARACTER VARYING(n)`, `CHAR VARYING(n)`
    and `NVARCHAR(n)`) holds at most `n` characters. `CHAR(n)` (also
    `CHARACTER(n)`, `NCHAR(n)` and `BPCHAR(n)`) holds exactly `n`: shorter
    values are padded with spaces. `CHAR` without a length is `CHAR(1)`, and
    `BPCHAR` without one is blank-padded with no limit. `VARCHAR` without a
    length, `VARCHAR(MAX)`, `TEXT`, `STRING` and `CLOB` have no limit.
    Lengths count characters, not bytes (`'日本語'` fits `VARCHAR(3)`), and go
    from 1 to 10485760.
  - **Writes:** a value longer than its column's length is an error, `value
    too long for type character varying(3)` (`character(3)` for `CHAR(3)`),
    with ADBC status `InvalidData`, unless the characters beyond the length
    are all spaces, which are cut (`'ab    '` is written to a `VARCHAR(3)` as
    `'ab '`). Every write checks it: `INSERT … VALUES`, `INSERT … SELECT`,
    `INSERT … DEFAULT VALUES`, `UPDATE` (also with `FROM`), both branches of
    `MERGE`, and bulk ingest. It is checked where `NOT NULL` and `CHECK` are,
    before them, so a statement with a failing row writes nothing (a bulk
    ingest is checked one Arrow batch at a time). A column's `DEFAULT` is
    checked when it is defined.
  - **Casts:** `CAST(x AS VARCHAR(n))` and `x::char(n)` cut the text to `n`
    characters (`CAST('abcdef' AS VARCHAR(3))` is `'abc'`), and `CHAR(n)`
    pads it (`CAST('ab' AS CHAR(3))` is `'ab '`). Only an explicit cast
    cuts; writing a longer value is an error.
  - **`CHAR` values** are stored and returned padded, and their trailing
    spaces don't count: `LENGTH` of `'ab '` in a `CHAR(3)` is 2, it equals
    `'ab'`, and joins, `IN`, `GROUP BY`, `DISTINCT` and set operations treat
    the two as the same value. Converting one to text (functions, `||`, a
    `VARCHAR` column) drops the padding (`c || 'x'` is `'abx'`), but `LIKE`,
    `ILIKE`, `~`, `~*` and `SIMILAR TO` see it (`c LIKE 'ab'` is false, `c
    LIKE 'ab%'` true), as in Postgres. One difference: a comparison with a
    `CHAR` value ignores trailing spaces on both sides, as Postgres does for
    a literal (`c = 'ab '` is true), so a text column holding `'ab '` also
    equals a `CHAR` holding `'ab'`; Postgres compares a `CHAR` with a text
    column as text, which keeps them.
  - **Result types** keep the length where Postgres does: a column, a cast,
    a scalar subquery, and `CASE`, `COALESCE`, `NULLIF`, `GREATEST`, `LEAST`
    or a set operation whose inputs all have the same type. Function,
    aggregate and window results have none, nor does a mix of lengths or a
    string literal (`COALESCE(s, 'x')`). So `CREATE TABLE … AS` and views
    keep the length of a column or cast they select, and a computed column
    is `VARCHAR`.
  - **Reading them back:** `GetObjects` reports the type as `VARCHAR(n)` /
    `CHAR(n)` in `xdbc_type_name`, with `xdbc_column_size` `n` and
    `xdbc_char_octet_length` `4n` (UTF-8); `information_schema.columns` has
    `character_maximum_length` and `character_octet_length` (NULL and
    1073741824 for a string without a length, as in Postgres) and the same
    type in `data_type`.
  - **Upgrading:** v0.0.7 and earlier didn't keep the length, so the tables
    they created have none and aren't checked; their values stay as they
    are. Only tables created and columns added from this version on are
    checked. `CHAR` without a length was unbounded, and is now `CHAR(1)`.
    Views, `CHECK`s and defaults that cast to `VARCHAR(n)` / `CHAR(n)` now
    cut. Clients on v0.0.7 and earlier ignore the length (an optional part
    of the column type in the metadata), so they don't check it, and a
    change they make to a table's metadata (an `ALTER TABLE`, a `COMMENT
    ON`, and some writes) removes it, so upgrade every client of a database
    together.
- Constraints, written as in Postgres. `NOT NULL`, `CHECK` and the lengths
  of `VARCHAR(n)` / `CHAR(n)` (see String lengths above) are enforced;
  `PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` are not:
  - **Accepted:** on a column (in `CREATE TABLE` and `ADD COLUMN`), any
    number of `NOT NULL`, `NULL`, `CHECK (expr)`, `DEFAULT expr`, `PRIMARY
    KEY`, `UNIQUE` and `REFERENCES t [(col)] [MATCH {FULL | PARTIAL |
    SIMPLE}] [ON DELETE action] [ON UPDATE action]`, in any order, each
    optionally named with `CONSTRAINT name`. As a table constraint (in
    `CREATE TABLE` and `ALTER TABLE … ADD`), `[CONSTRAINT name]` followed by
    `CHECK (expr)`, `PRIMARY KEY (cols)`, `UNIQUE (cols)` or `FOREIGN KEY
    (cols) REFERENCES t [(cols)] …`. A column `CHECK` means the same as a
    table one: a condition on the whole row, which may read any column.
  - **Not enforced:** `PRIMARY KEY`, `UNIQUE` and `FOREIGN KEY` /
    `REFERENCES` are accepted and ignored. Nothing is kept, so duplicate
    keys and rows without a parent are inserted, the referenced table
    needn't exist, and `GetObjects` doesn't list them.
  - **`CHECK`, when written:** the expression is checked as in Postgres. It
    may read the table's columns (`col` or `t.col`) but not `__rowid` or
    another table, may not use subqueries, parameters, aggregates,
    `GROUPING` or window functions, and must be boolean. Its function calls
    are checked as in a query (see "Function calls" below). The errors are
    `column "x" does not exist in table "t"`, `system column "__rowid"
    reference in check constraint is invalid`, `missing FROM-clause entry
    for table "u"`, `cannot use subquery in check constraint`, `cannot use
    parameter in check constraint`, `aggregate functions are not allowed in
    check constraints`, `grouping operations are not allowed in check
    constraints`, `window functions are not allowed in check constraints`,
    `function nosuchfunc(integer) does not exist`, `UPPER expects 1
    argument` and `argument of CHECK must be type boolean, not type
    integer`. A value the expression can't compute for a row (`LOG(0)`) is
    an error when that row is written.
  - **Names:** `CONSTRAINT name` is used as written. Without one, the name
    is Postgres's: `<table>_<column>_check` if the expression reads exactly
    one column (`t_amount_check`, also for a table constraint such as
    `CHECK (id > 0)`), otherwise `<table>_check`, with `1`, `2`, … appended
    to a name the table already uses (`t_amount_check1`). Two constraints
    of a table can't have the same name (`check constraint "c" already
    exists`; `ADD`: `constraint "c" for relation "t" already exists`).
  - **Enforcement:** every write checks each new or changed row: `INSERT …
    VALUES`, `INSERT … SELECT`, `INSERT … DEFAULT VALUES`, `UPDATE` (also
    with `FROM`), both branches of `MERGE`, and bulk ingest. As in SQL, a
    row fails only if the expression is FALSE; NULL passes. The row is
    checked after its defaults are filled in and after `NOT NULL`, with
    the error `new row for relation "t" violates check constraint
    "t_amount_check"`. Like `NOT NULL`, this happens while the statement
    computes its changes, so a statement with a failing row writes
    nothing; a bulk ingest is checked one Arrow batch at a time, so the
    batches before a failing one stay written. As in Postgres, the
    constraints are checked in name order, so a row that fails several
    reports the first, and an `UPDATE` checks every constraint, also those
    that don't read the columns it sets (it reads the columns they use).
  - **Existing rows:** `ADD COLUMN … CHECK` and `ADD [CONSTRAINT name] CHECK`
    check every row first, with the new column at its default, and fail
    with `check constraint "c" of relation "t" is violated by some row`.
    Rows that other connections write while that runs aren't checked
    against the new constraint.
  - **Lifetime:** the constraints are part of the table's metadata (an
    optional `checks` field holding each name and expression, so older
    metadata reads unchanged). `RENAME TO` (also with
    `adbc.redis.rename_rekey`) and `TRUNCATE` keep them, with their names;
    errors name the table as it is now. `RENAME COLUMN` rewrites the
    expressions that read the column. `DROP COLUMN` drops the constraints
    that read it, also those that read other columns too, and `DROP
    CONSTRAINT [IF EXISTS] name` drops one. Temporary tables have them in
    the same way; `CREATE TABLE … AS` copies none.
  - **Reading them back:** `GetObjects`' `table_constraints` lists each
    `CHECK`, with type `CHECK` and the columns its expression reads.
  - **Upgrading:** clients on v0.0.6 and earlier ignore the new metadata.
    They don't check the constraints, and a change they make to a table's
    metadata (an `ALTER TABLE`, a `COMMENT ON`, and some writes) removes
    them, so upgrade every client of a database together. Earlier versions
    accepted table-level `CHECK`s without storing them, so tables they
    created don't enforce theirs; add them again with `ALTER TABLE … ADD
    CHECK (…)`. Earlier versions also accepted a constraint that calls an
    unknown function; every write to its table now fails with `check
    constraint "c" of relation "t" can't be evaluated (function
    nosuchfunc(integer) does not exist); drop it with ALTER TABLE … DROP
    CONSTRAINT`.
- `[WITH [RECURSIVE] name [(cols)] AS (SELECT …), …] SELECT [ALL | DISTINCT | DISTINCT ON (…)] … FROM item {, item |
  [INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item ON … | USING (…) | CROSS JOIN item |
  NATURAL [INNER | LEFT | RIGHT | FULL] [OUTER] JOIN item}
  [WHERE …] [GROUP BY …] [HAVING …] [WINDOW w AS (…), …] [QUALIFY …]
  [ORDER BY …] [LIMIT n] [OFFSET m [ROW | ROWS]] [FETCH {FIRST | NEXT} [n] {ROW | ROWS} ONLY]`,
  where an item is a table, a CTE, `[LATERAL] (SELECT …)` or
  `[LATERAL] GENERATE_SERIES(…)`, each with an optional alias and column
  aliases (`AS a(x, y)`; `t.col` qualifies a column, and `t.*` selects one
  item's columns, also as `schema.table.*`),
  with aggregates (below), `CASE` (simple and searched), arithmetic,
  `CAST(x AS type)` / `x::type` (and `TRY_CAST`, see below), `IS [NOT] NULL`,
  `IS [NOT] DISTINCT FROM` (NULLs count as equal), row constructors
  (`(a, b) = (1, 'x')`, `(a, b) IN (SELECT …)`; see below), `[NOT] LIKE` / `ILIKE` (with `ESCAPE`), subqueries (scalar `(SELECT …)`, `EXISTS`,
  `[NOT] IN (SELECT …)`, `x op ANY | SOME | ALL (SELECT …)`, also with a row
  on the left, correlated or not, in SELECT/WHERE/HAVING and in
  `UPDATE`/`DELETE`/`MERGE`),
  `BETWEEN`, `IN`, `COALESCE`, `LOWER/UPPER/LENGTH/ABS`, `CONCAT(a, …)` and
  `CONCAT_WS(sep, a, …)` (NULL arguments are skipped, as in Postgres; `||`
  returns NULL if either side is NULL), `from_hex`
- `FETCH FIRST n ROWS ONLY` is `LIMIT n` (`FETCH FIRST ROW ONLY` is `LIMIT
  1`); `WITH TIES` is not supported, and a query has at most one `LIMIT` or
  `FETCH` (Postgres's "multiple LIMIT clauses not allowed")
- A query that can't return rows (`WHERE false`, `LIMIT 0`, …; see "How SQL
  is executed") reads none. It is still planned, so unknown tables, columns
  and functions and wrong argument counts are errors. Errors the driver
  raises only when it computes a value on a row (division by zero, a failed
  cast, the logarithm of zero) are then not raised, as on an empty table:
  `SELECT 1/0 FROM t WHERE false` returns no rows
- `GROUP BY` items are expressions, output positions or aliases, and also
  (as in Postgres) `ROLLUP (…)`, `CUBE (…)`, `GROUPING SETS (…)` and `()`:
  - `ROLLUP (a, b)` is the grouping sets `(a, b), (a), ()`, and `CUBE (a, b)`
    is every subset of its items (at most 12). `GROUPING SETS (…)` lists
    sets, and may contain `ROLLUP`, `CUBE` and `GROUPING SETS`. A
    parenthesized list is one item of several expressions (`ROLLUP (a, (b,
    c))`), and `()` is the empty set: one group of all the rows, even when
    there are none
  - Several items combine as a cross product: `GROUP BY a, ROLLUP (b, c)` is
    `(a, b, c), (a, b), (a)`. At most 4,096 sets; repeated sets are kept
  - A set's rows have NULL for the grouping columns it doesn't group.
    `GROUPING(a, …)` tells those from NULL values: an `INTEGER` with a bit
    per argument, the last one lowest, set when the argument is not grouped.
    It works in the SELECT list, HAVING, ORDER BY, QUALIFY and window
    definitions (and is 0 in a plain `GROUP BY`)
  - As in Postgres, outside aggregate calls such a query may use only
    grouping expressions, matched as a whole (`GROUP BY ROLLUP (UPPER(s))`
    can select `UPPER(s)` but not `s`); this also applies to a plain `GROUP
    BY` that calls `GROUPING()`
- `UNION [ALL]`, `INTERSECT [ALL]`, `EXCEPT [ALL]` (`INTERSECT` binds
  tighter; parenthesized branches may have their own `ORDER BY`/`LIMIT`).
  Columns are matched by position and widened to a common type; NULLs count
  as equal when removing duplicates. `ORDER BY` on the combined result uses
  output column names or positions. Set operations work anywhere a query
  does (subqueries, CTEs, views, CTAS, `INSERT … SELECT`)
- `WITH RECURSIVE name [(cols)] AS (non-recursive term UNION [ALL]
  recursive term)`, evaluated as in Postgres: the recursive term reads the
  rows the previous iteration added until it adds none, and `UNION` also
  drops rows equal to any earlier row (NULLs count as equal). The columns
  are named by the CTE's column list or the non-recursive term, and have the
  non-recursive term's types, widened to hold the recursive term's values of
  the same category (otherwise it's Postgres's "has type … in non-recursive
  term but type … overall" error; cast the non-recursive term). Postgres's
  rules apply, with its errors: the recursive term refers to the CTE once,
  not in a subquery, on the NULL-supplying side of an outer join, under
  `EXCEPT` or `INTERSECT ALL`, or at a level with aggregates; no `ORDER BY`
  / `LIMIT` / `OFFSET` on the `UNION`; no mutual recursion between CTEs. The
  CTE is computed in full before the query reads it (Postgres stops early,
  for instance under a `LIMIT`), so the recursive term needs a stop
  condition: more than 10,000 iterations that add rows, or more than
  1,000,000 rows, is an error. A CTE of a `WITH RECURSIVE` list that doesn't
  refer to itself is an ordinary CTE
  - `SEARCH {BREADTH | DEPTH} FIRST BY col, … SET seq` adds a `BIGINT` column
    that orders the rows breadth or depth first (Postgres has a record or an
    array that sorts the same way); `CYCLE col, … SET mark [TO v DEFAULT d]
    USING path` marks (and doesn't recurse into) a row whose columns repeat a
    row on its path, and adds the path as text in Postgres's format
    (`{(1),(2)}`). As in Postgres, the recursive term must then be a SELECT
    that reads the CTE at its top level; here it also can't use `GROUP BY`,
    `HAVING`, window functions, `LIMIT` or `OFFSET`
- `LATERAL (SELECT …)` in FROM (`, LATERAL`, `CROSS JOIN LATERAL`,
  `[LEFT] JOIN LATERAL … ON …`) reads the columns of the items before it; a
  table function reads them with or without `LATERAL`, as in Postgres. It
  runs once per distinct value of the columns it reads. `RIGHT` and `FULL`
  joins can't have such a reference (Postgres's error), but are fine for a
  `LATERAL` item that reads none
- `NATURAL [INNER | LEFT | RIGHT | FULL] JOIN` joins on the columns both
  sides have (a cross join if none). As in Postgres, `SELECT *` lists each
  common column once, first, then the left side's other columns and the
  right side's; the merged column is the left one for inner and left joins,
  the right one for right joins and `COALESCE` of both for full joins, and
  an unqualified reference means the merged column (`a.id` still reads
  `a`'s). `USING (…)` keeps its existing behavior (both columns, so an
  unqualified reference is ambiguous)
- `GENERATE_SERIES(start, stop [, step])` in FROM, with Postgres's overloads:
  integers (step 1 by default) and numerics, and timestamps with an interval
  step (`DATE` arguments give `TIMESTAMP WITH TIME ZONE`, as in Postgres;
  cast with `::date`). The column is named `generate_series`, or after the
  alias (`AS g` → `g`, `AS g(n)` → `n`). A zero step is an error, a step in
  the wrong direction or a NULL argument gives no rows, and month steps
  clamp (`Jan 31, Feb 29, Mar 29, …`). Parameters work, and so do joins
  (`generate_series(…) d LEFT JOIN t ON t.day = d::date` for a date spine)
  and arguments read from earlier FROM items. At most 1,000,000 rows per
  call
- `x op ANY | SOME | ALL (SELECT …)` for `= <> < <= > >=`, with SQL's
  three-valued logic: `ANY` is true if a comparison is, else NULL if one is
  NULL; `ALL` is false if a comparison is, else NULL if one is NULL. Over no
  rows `ANY` is false and `ALL` true. `= ANY` is `IN` and `<> ALL` is
  `NOT IN`
- Row constructors, as in Postgres: `(a, b, …)` with two or more items, and
  `ROW(…)` with any number (`ROW(a)`, `ROW()`). They can be compared
  wherever a condition can be: `WHERE`, `ON`, `HAVING`, `CASE`, the SELECT
  list, `CHECK`, and the conditions of `UPDATE`, `DELETE` and `MERGE`:
  - **`=` and `<>`:** item by item. `=` is true if every pair is equal,
    false if a pair is unequal, and otherwise NULL: `(1, NULL) = (1, 2)` is
    NULL, `(1, NULL) = (2, 2)` false. `<>` is its negation
  - **`<`, `<=`, `>`, `>=`:** left to right; the first pair that is unequal
    or has a NULL decides, NULL for a NULL. `(1, 2) < (1, 3)` and
    `(2, NULL) > (1, 5)` are true, `(1, NULL) < (1, 5)` is NULL. Keyset
    pagination writes `WHERE (ts, id) > (?, ?)`
  - **`IS [NOT] DISTINCT FROM`:** item by item, with NULLs equal
  - **`IS [NOT] NULL`:** `(a, b) IS NULL` is true when every item is NULL,
    `IS NOT NULL` when none is (so a row can be neither)
  - **`[NOT] IN`:** over a list of rows, or a subquery with as many columns
    as the row has items, with three-valued logic: if no row is equal but a
    comparison is NULL (`(1, NULL)` with `(1, 2)`), `IN` is NULL rather than
    false and `NOT IN` NULL rather than true. So dbt's
    `delete+insert` strategy with a list `unique_key`, `delete from t where
    (k1, k2) in (select distinct k1, k2 from …)`, keeps the rows of `t`
    with a NULL key column, as on Postgres
  - **`ANY` / `SOME` / `ALL`** over a subquery: `= ANY` and `= SOME` are
    `IN`, `<> ALL` is `NOT IN`, and the other operators compare with each
    row
  - **`(a, b) op (SELECT x, y …)`** compares with the subquery's row: NULL
    if it returns none, an error if it returns more than one
  - **`BETWEEN` and simple `CASE`** work with rows too (`(a, b) BETWEEN (1,
    2) AND (3, 4)`, `CASE (a, b) WHEN (1, 2) THEN …`). `EXISTS` is
    unchanged
  - **Errors**, raised when the statement is planned, are Postgres's:
    `unequal number of entries in row expressions`, `subquery has too many
    columns` / `subquery has too few columns` (also `x IN (SELECT a, b …)`
    for a single value), `cannot compare rows of zero length`, and
    `operator does not exist: record = integer` for a row compared with a
    single value. A row anywhere else (as a select list item, a function
    argument, in arithmetic) is `a row constructor can only be compared (…)
    or tested with IS [NOT] NULL`, and a row nested in another is `nested
    row constructors are not supported` (Postgres compares those as
    composite values, with other NULL rules). Rows as values (`SELECT (a,
    b)` returning a composite) and `ORDER BY` / `GROUP BY` on a row
    expression aren't supported
  - A comparison of two rows is rewritten into comparisons of their items,
    so `(k1, k2) = (1, 'x')` runs in the index like `k1 = 1 AND k2 = 'x'`.
    For `IN`, see "How SQL is executed"
- `ORDER BY … [ASC|DESC] [NULLS FIRST|LAST]`; NULLs sort last by default in
  both directions
- Function calls (the functions are listed below) are checked when the
  statement is planned, before any row is read, wherever a call can be:
  the SELECT list, `WHERE`, `GROUP BY`, `HAVING`, `QUALIFY`, `ORDER BY`,
  `ON`, window definitions, subqueries, CTEs and derived tables, the body
  of `CREATE VIEW`, `INSERT`, `UPDATE`, `DELETE`, `MERGE` and `RETURNING`,
  and `CHECK` and `DEFAULT` expressions when they are defined. So the errors
  don't depend on whether a table has rows, and a query that reads none
  (`WHERE false`, dbt's `where false limit 0`, `ExecuteSchema`) gets them
  too:
  - Names are case-insensitive. A quoted name (`"upper"(x)`) or a
    schema-qualified one (`pg_catalog.upper(x)`) is not a call (a syntax
    error), except `"generate_series"(…)` in FROM
  - An unknown function is Postgres's `function nosuchfunc(integer) does
    not exist`, with the argument types the planner infers (`unknown` for
    NULL and for a parameter without a type)
  - A wrong argument count is `UPPER expects 1 argument`, `ROUND expects 1
    or 2 arguments`, `REGEXP_INSTR expects 2 to 7 arguments`, `GREATEST
    expects at least 1 argument` or `RANDOM expects no arguments`, also
    when the arguments are NULL. Only `COUNT` takes `*` (`SUM does not
    accept *`), and only aggregates take `DISTINCT` (`UPPER does not
    accept * or DISTINCT`)
  - Where an argument's type decides whether the call exists, it is
    checked too: `BOOL_OR(int_col)` is `function bool_or(integer) does not
    exist`, and the offset of `LAG` must be an integer
  - As in Postgres, the tables of a statement are resolved before its
    expressions, and an unknown column or function in a call's arguments
    is reported before the call's own error
  - Errors that depend on values (division by zero, a failed cast,
    `LOG(0)`) are still raised when a row is evaluated, as in Postgres
  - A view stored by an earlier version whose body calls an unknown
    function fails whenever it is read, also when its tables are empty:
    `view "v" is no longer valid: function nosuchfunc(integer) does not
    exist`. `CREATE OR REPLACE VIEW` or `DROP VIEW` fixes it
  - A CTE that no part of the statement reads isn't planned, so its calls
    (like its columns) aren't checked
  - `information_schema.routines` lists the functions, each alias too:
    `routine_name` (lower case), `routine_schema` (`pg_catalog`, though a
    call can't name it), `routine_type` (`FUNCTION`), `function_kind`
    (`SCALAR`, `AGGREGATE`, `WINDOW` or `TABLE`), `min_arguments`,
    `max_arguments` (NULL for no maximum) and `alias_of` (the function an
    alias names, NULL otherwise)
- Aggregates, with `GROUP BY` or over the whole input. They skip NULL
  inputs, and all but `COUNT` give NULL when there are none:
  - `COUNT(*)`, `COUNT/SUM/AVG/MIN/MAX(x)`, `COUNT(DISTINCT x)`, and
    `COUNT(DISTINCT a, b, …)` (as in MySQL), which counts the distinct
    combinations of rows where none of them is NULL
  - `SUM`, `AVG` and the variances and standard deviations add their
    values exactly, whatever their type, also as window functions: every
    integer, decimal and double is a fraction whose denominator is a power
    of 2 times a power of 5, so the driver keeps the sum as such a fraction
    and rounds the result once, to the nearest double (ties to even), or to
    the result's decimal places. So the result is the same in any row
    order: on a cluster, rows come in a different order each time, and
    summing doubles in that order used to give a different last digit from
    one run to the next. Postgres also adds doubles one by one, in its plan's
    order.
    - `SUM` of integers is `BIGINT` and fails (`integer overflow in SUM`)
      only if the total doesn't fit, and `SUM` of decimals is exact at
      their scale. `AVG` (`DOUBLE PRECISION`) is the exact mean, rounded
      once, also for integers beyond 2^53 and for decimals
    - Doubles: a NaN, or both infinities, make `SUM` and `AVG` NaN, and one
      infinity makes them that infinity, as adding them in any order would;
      the variances are then NaN, as in Postgres. A finite sum beyond the
      largest double is an infinity, as before (Postgres raises "value out
      of range: overflow")
    - Adding a value costs an integer addition, about 25 ns for a double
      (40 ns with the squares a variance needs), against 3–7 µs to read a
      row
    - **Upgrading:** results of v0.0.7 and earlier can differ in their last
      digits: `SUM`, `AVG` and the variances of doubles, `AVG` of decimals,
      and `AVG` of integers whose sum is beyond 2^53. The new value is the
      correctly rounded one. `SUM` of integers no longer fails when a running
      total overflows and the final one fits, and a decimal `SUM` whose
      values include integers (`SUM(CASE WHEN … THEN 1 ELSE 2.5 END)`) no
      longer leaves the integers out
  - `STRING_AGG(x, sep [ORDER BY …])`, and `LISTAGG(x [, sep]) [WITHIN GROUP
    (ORDER BY …)]` (Snowflake, Oracle; `sep` defaults to `''`);
    `STRING_AGG(x, sep) WITHIN GROUP (ORDER BY …)` also works (SQL Server).
    Values of any type are joined as text. As in PostgreSQL, each value but
    the first is preceded by its own row's separator, and with `DISTINCT`
    (whose `ORDER BY` may only use the arguments) the values come out sorted
  - `BOOL_OR(b)`, `BOOL_AND(b)` / `EVERY(b)` on booleans, `ANY_VALUE(x)` (a
    non-NULL value of the group)
  - `VAR_SAMP` / `VARIANCE`, `VAR_POP`, `STDDEV_SAMP` / `STDDEV`,
    `STDDEV_POP` return `DOUBLE PRECISION` (PostgreSQL returns `numeric` for
    integer and numeric inputs). The values and their squares are summed
    exactly, so the result is the correctly rounded double whatever the
    values' type and size. The sample versions are NULL for one value, the
    population ones 0
  - Ordered-set aggregates: `PERCENTILE_CONT(f) WITHIN GROUP (ORDER BY x)`
    interpolates (numbers as doubles, and intervals), `PERCENTILE_DISC(f)
    WITHIN GROUP (ORDER BY x)` returns the first value at or past fraction
    `f` (any sortable type), `MODE() WITHIN GROUP (ORDER BY x)` the most
    frequent value (the first in sort order among ties), with PostgreSQL's
    formulas and errors. `MEDIAN(x)` is `PERCENTILE_CONT(0.5) WITHIN GROUP
    (ORDER BY x)`. The fraction is evaluated once per group (a constant, a
    parameter or a grouped column) and must be between 0 and 1
  - `agg(…) FILTER (WHERE cond)` on any aggregate, also as a window function:
    only the rows where `cond` is true are aggregated, and their arguments
    alone are evaluated
  - Aggregate `ORDER BY` matters only for `STRING_AGG` / `LISTAGG`; its NULLs
    sort last by default in either direction, as in `ORDER BY`
- Window functions, `fn(…) OVER ([PARTITION BY …] [ORDER BY …] [frame])`, in
  the SELECT list, `ORDER BY` and `QUALIFY`. They see the rows after `WHERE`,
  `GROUP BY` and `HAVING`, and work anywhere a query does (joins, subqueries,
  CTEs, views, CTAS, `INSERT … SELECT`):
  - Ranking: `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `PERCENT_RANK`, `CUME_DIST`,
    `NTILE(n)`
  - Offset: `LAG` / `LEAD(x [, offset [, default]])`, `FIRST_VALUE(x)`,
    `LAST_VALUE(x)`, `NTH_VALUE(x, n)`, each with `IGNORE NULLS` or `RESPECT
    NULLS` (the default) after the call (SQL standard, Snowflake) or after
    its last argument (BigQuery, DuckDB). `IGNORE NULLS` counts only the rows
    whose value is not NULL, so `LAST_VALUE(x) IGNORE NULLS OVER (ORDER BY
    ts)` fills forward; `LAG(x, 0)` is still the current row's value
  - Aggregates: `COUNT(*)`, `COUNT/SUM/AVG/MIN/MAX(x)`, `STRING_AGG`,
    `LISTAGG`, `BOOL_OR`, `BOOL_AND`, `EVERY`, `ANY_VALUE`, and the variances
    and standard deviations, with `FILTER (WHERE …)`, also over grouped
    results (`SUM(COUNT(*)) OVER (ORDER BY day)`). As in PostgreSQL,
    `DISTINCT`, an aggregate `ORDER BY` (order the window instead) and the
    ordered-set aggregates and `MEDIAN` are not supported with `OVER`
  - Frames: `{ROWS | RANGE | GROUPS} {start | BETWEEN start AND end}` with
    `UNBOUNDED PRECEDING`, `n PRECEDING`, `CURRENT ROW`, `n FOLLOWING`,
    `UNBOUNDED FOLLOWING`, and `EXCLUDE CURRENT ROW | GROUP | TIES | NO
    OTHERS` (`GROUP` removes the current row and its peers, `TIES` only its
    peers). `RANGE` offsets are numbers for a numeric `ORDER BY` key and
    intervals for dates, times, timestamps and intervals; as in PostgreSQL,
    a `TIME` key moves by the interval's time part and its frames don't wrap
    around midnight. Without a frame, the SQL default applies: with `ORDER
    BY`, from the start of the partition to the current row's last peer (rows
    with equal keys); without `ORDER BY`, the whole partition
  - Named windows: `WINDOW w AS (…)`, `OVER w`, `OVER (w ORDER BY … [frame])`
  - `QUALIFY` filters on window results and may use output aliases
    (`QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) = 1`)
  - Result types: `BIGINT` for `ROW_NUMBER`, `RANK`, `DENSE_RANK`, `NTILE` and
    `COUNT`; `DOUBLE PRECISION` for `PERCENT_RANK`, `CUME_DIST` and `AVG`; the
    argument's type for `MIN`, `MAX` and the offset functions; the other
    aggregates as with `GROUP BY`
  - As in `ORDER BY`, NULLs sort last by default in either direction
    (PostgreSQL puts them first for `DESC`). Rows that tie on the window's
    `ORDER BY` keep their input order
- Dates, times and timestamps become text (`CAST(x AS VARCHAR)`, `||`,
  `CONCAT`, `STRING_AGG`, `LIKE`, `MD5(CAST(x AS TEXT))`, …) as Postgres
  writes them, whatever the declared precision: seconds always, then the
  fraction without trailing zeros (`2024-01-10 10:00:00`,
  `2024-01-10 10:00:00.5`, `10:00:00.05`), the offset of a timestamp with
  time zone (`2024-01-10 10:00:00+00`), a year before 1 AD as a BC year
  (`0044-03-15 BC`), and a year past 9999 with all its digits
  (`10000-01-01`). Text in those forms reads back as the same value, and
  there is no year 0. JSON (`TO_JSON`, `JSON_BUILD_OBJECT`, …) has the ISO
  8601 form, `2024-01-10T10:00:00.5+00:00`. Intervals are written as in
  Postgres too, with a `+` on a field after a negative one (`-1 days
  +02:00:00`). Query results are Arrow values, which this doesn't change
  - **Upgrading from v0.0.7 and earlier:** they kept the declared
    precision's trailing zeros (`2024-01-10 10:00:00.000000`, `10:00:00.000`,
    `2024-01-10 10:00:00.000+00`), wrote years before 1 AD as `0000-…` or
    `-0001-…`, and left out an interval's `+`. The text of such values
    changes, and with it every hash of the text: dbt_utils'
    `generate_surrogate_key` over a timestamp or time column, and the
    `dbt_scd_id` of dbt snapshots (an `md5` of the key and `updated_at` as
    text). Keys that earlier versions stored don't match the ones computed
    now (which match Postgres's): an incremental model whose `unique_key`
    is such a key inserts its rows again instead of updating them, and a
    snapshot whose `unique_key` is one sees every row as new. Rebuild those
    (`dbt run --full-refresh`, or drop the snapshot's table and run `dbt
    snapshot`) before running them with this version. Snapshots only read
    their stored `dbt_scd_id`s back, so they keep working; the rows they
    add from now on have ids in the new form. Text stored by earlier
    versions (`CAST(ts AS VARCHAR)` written to a column) keeps the old form,
    and still reads back as the same values, except for years before 1 AD
- Casts: `CAST(x AS type)` and `x::type` fail on a value that doesn't
  convert (text that doesn't parse, a value out of the type's range,
  overflow). `TRY_CAST(x AS type)` and `SAFE_CAST(x AS type)` return NULL
  for it instead, and `CAST(x AS type DEFAULT v ON CONVERSION ERROR)` returns
  `v` (converted to the type). The result has the target type. An error
  while computing `x`, and a cast between types that never convert (`DATE`
  to `BOOLEAN`), are still errors. A cast to `VARCHAR(n)` or `CHAR(n)` cuts
  the text to `n` characters, and `CHAR(n)` pads it (see String lengths)
- Math functions: `ROUND(x [, n])` and `TRUNC(x [, n])` (`n` may be
  negative: `ROUND(1250, -2)` is 1300), `FLOOR`, `CEIL` / `CEILING`, `MOD` / `%`,
  `POWER` / `POW`, `SQRT`, `CBRT`, `LN`, `LOG(x)` (base 10) / `LOG(b, x)`,
  `LOG10`, `EXP`, `SIGN`, `ABS`, `PI()`, `RANDOM()`
  - `ROUND` rounds half away from zero: exactly on `NUMERIC`, and on
    `DOUBLE PRECISION` too, by the value as written (`ROUND(2.675e0, 2)` is
    2.68). Postgres rounds doubles half to even
  - Integers and doubles keep their type. `ROUND(NUMERIC(p,s), n)` has scale
    `n` (at most `s`); `ROUND(x)`, `TRUNC(x)`, `FLOOR` and `CEIL` scale 0.
    `MOD` and `%` have their arguments' common type (`NUMERIC` stays
    exact); `SQRT`, `CBRT`, `LN`, `LOG`, `EXP`, `POWER`, `PI` and `RANDOM`
    return `DOUBLE PRECISION`
  - Errors as in Postgres for the square root of a negative number, the
    logarithm of zero or of a negative number, `MOD` / `%` by zero (of any
    numeric type), zero to a negative power, and overflow. `EXP` of a double
    that underflows is an error too ("value out of range: underflow"); `EXP`
    of a `NUMERIC` then returns 0, as Postgres's `exp(numeric)` does
- Trigonometric functions, as in Postgres, all `DOUBLE PRECISION`:
  - In radians: `SIN`, `COS`, `TAN`, `COT`, `ASIN`, `ACOS`, `ATAN`,
    `ATAN2(y, x)`; `RADIANS(degrees)` and `DEGREES(radians)` convert
  - In degrees: `SIND`, `COSD`, `TAND`, `COTD`, `ASIND`, `ACOSD`, `ATAND`,
    `ATAN2D(y, x)`. With Postgres's algorithm, they are exact where the
    result is a simple number: `SIND(30)` is 0.5, `COSD(60)` 0.5, `TAND(45)`
    1, `ASIND(0.5)` 30, `ATAN2D(1, 1)` 45, and `TAND(90)` and `COTD(0)` are
    Infinity
  - Arguments are numbers (or text that converts); a NULL argument gives
    NULL and a NaN gives NaN. Errors as in Postgres: `ASIN: input is out of
    range` for an argument beyond [-1, 1] of `ASIN`, `ACOS`, `ASIND` and
    `ACOSD`, and for an infinite one of the sines, cosines, tangents and
    cotangents; `DEGREES` can overflow and `RADIANS` underflow ("value out of
    range: …")
  - The radian functions are Go's `math` package. Postgres uses the
    platform's C library, which can differ in the last bit: `TAN(1)` is
    1.557407724654902 here and 1.5574077246549023 with glibc
  - So dbt_utils' `haversine_distance` works, in its default form (with
    `radians`) and in its BigQuery form (with `acos(-1)`)
- String functions; positions are 1-based and count characters, not bytes:
  - `SUBSTRING(s, start [, len])`, `SUBSTRING(s FROM start [FOR len])`
    (with text arguments, the pattern forms below),
    `SUBSTR`, `LEFT(s, n)` / `RIGHT(s, n)` (a negative `n` drops characters
    from the other end), `POSITION(sub IN s)`, `STRPOS(s, sub)`
  - `TRIM([BOTH | LEADING | TRAILING] [chars] FROM s)`, `TRIM(s [, chars])`,
    `LTRIM`, `RTRIM`, `BTRIM` (spaces by default), `LPAD` / `RPAD(s, len
    [, fill])`, `REPLACE`, `REVERSE`, `REPEAT`, `INITCAP`, `MD5`,
    `STARTS_WITH`, `SPLIT_PART(s, delim, n)` (a negative `n` counts from
    the end)
  - `REGEXP_REPLACE(s, pattern, replacement [, start [, n]] [, flags])`
    replaces the first match, or all of them with flag `g`; with Postgres
    16's `start` and `n`, the `n`-th match from `start` (all of them if `n`
    is 0). `\1` … `\9` and `\&` in the replacement insert groups and the
    match. Flags and pattern syntax as below
- Regular expressions, with Postgres's arguments, results and errors:
  - `s ~ pattern`, `s ~* pattern` (case-insensitive), `s !~ pattern`,
    `s !~* pattern`. As in Postgres, they bind like `||`: tighter than
    comparisons, `IS`, `LIKE`, `BETWEEN` and `IN`, looser than arithmetic
    (`s ~ 'a' || 'b'` is `(s ~ 'a') || 'b'`)
  - `REGEXP_LIKE(s, pattern [, flags])`, `REGEXP_COUNT(s, pattern [, start
    [, flags]])`, `REGEXP_INSTR(s, pattern [, start [, n [, endoption
    [, flags [, subexpr]]]]])` and `REGEXP_SUBSTR(s, pattern [, start [, n
    [, flags [, subexpr]]]])`, as in Postgres 15 (Snowflake's have the same
    order): the `n`-th match from position `start`, or its group `subexpr`.
    A search from `start` sees the text before it, so `^` does not match
    there
  - `REGEXP_MATCH(s, pattern [, flags])` returns the first match's groups
    (or the whole match if there are none) as the text of Postgres's
    `text[]` result, `'{bar,beque}'`, since the driver has no array type.
    `REGEXP_SUBSTR(s, pattern, 1, 1, '', n)` returns group `n` alone
  - `SUBSTRING(s FROM pattern)` returns the first group of the first match,
    or the whole match. As in Postgres, the argument types choose the form:
    text is a pattern and an integer a position, so `SUBSTRING(s, '2')` is a
    pattern match and `SUBSTRING(s FROM 2 FOR 3)` a position
  - `[NOT] SIMILAR TO pattern [ESCAPE e]` and `SUBSTRING(s SIMILAR pattern
    ESCAPE e)` (also `SUBSTRING(s FROM pattern FOR e)`) translate the SQL
    pattern as Postgres does. It must match the whole string, `%` and `_`
    are wildcards, `|`, `*`, `+`, `?`, `{m,n}`, `( )` and `[ ]` keep their
    meaning, and the escape character (backslash by default, none with
    `ESCAPE ''`) makes the next character literal (an escaped letter is an
    RE2 escape, such as `\d` for a digit). `SUBSTRING … SIMILAR`
    returns the part between the two `e"` separators, where the part before
    them matches as little of the string as it can
  - Flags: `i` (case-insensitive), `c`, `n` / `m` (newline-sensitive), `s`,
    `p`, `w`, `q` (a literal pattern), and `g` for `REGEXP_REPLACE` only;
    `x`, `b` and `e` are not supported. Without flags, `.` matches newlines
    and `^` and `$` anchor the whole string
  - Patterns use Go's RE2 syntax: no backreferences (`\1`) or lookaround,
    and `\b` is a word boundary (`\y` in Postgres). Where a pattern can match
    text of different lengths at the same position, RE2 takes the first
    alternative, as Perl does, so `SUBSTRING('xyz' FROM 'x|xy')` is `x`
    where Postgres gives `xy`. A pattern that doesn't compile is an error
    such as `` invalid regular expression: missing closing ): `(` ``
  - Patterns over columns are evaluated by the driver, on the rows the rest
    of the `WHERE` clause finds in the index: `~ '^abc'` and
    `SIMILAR TO 'abc%'` are not index prefix queries. `REGEXP_MATCHES` and
    `REGEXP_SPLIT_TO_TABLE` / `REGEXP_SPLIT_TO_ARRAY` (sets and arrays) are
    not supported
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
- Date/time functions. The session time zone is UTC: a `TIMESTAMP WITH
  TIME ZONE` is shown in UTC, and the current time is fixed once per
  statement:
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
  - `DATEADD(part, n, x)` and `DATEDIFF(part, a, b)` (Snowflake, Redshift,
    SQL Server; what dbt's `dateadd` and `datediff` emit), with `part` a bare
    keyword or a string: `year`, `quarter`, `month`, `week`, `day`, `hour`,
    `minute`, `second`, `millisecond`, `microsecond`, their plurals, and
    abbreviations such as `yy` / `yyyy`, `qq`, `mm` / `mon`, `wk` / `ww`,
    `dd` / `d`, `hh`, `mi` / `n` / `m`, `ss` / `s`, `ms`, `us` / `mcs` (`m`
    is minute and `w` is week, as in Snowflake). In the part's position a
    bare part name is the part even if a column has that name (write `"day"`
    for the column); everywhere else it is the column
  - `DATEDIFF` counts boundaries crossed, exactly like `DATE_DIFF`.
    `DATEADD` adds `n` parts (a non-integer `n` is rounded, as by
    `CAST(n AS BIGINT)`); months clamp to the end of the month. As in
    Snowflake, a date stays a date for a part of a day or longer and becomes
    a timestamp for a smaller one; timestamps and times keep their type
    (times wrap around midnight, and date parts aren't valid for them), and
    text is read as a timestamp
  - Aliases: `TIMESTAMPADD` / `TIMESTAMPDIFF` (as in Snowflake; MySQL's
    `TIMESTAMPDIFF` counts whole units elapsed instead),
    `DATE_ADD(part, n, x)` (Trino, Databricks), and
    `DATE_ADD(x, INTERVAL n part)` / `DATE_SUB(x, INTERVAL n part)` (MySQL,
    BigQuery), which are `DATEADD(part, n, x)` / `DATEADD(part, -n, x)`
  - `MAKE_DATE`, `MAKE_TIME`, `MAKE_TIMESTAMP`, `MAKE_TIMESTAMPTZ`,
    `TO_TIMESTAMP(epoch_seconds)`, `EPOCH(x)`, `EPOCH_MS(x)`
  - `TO_CHAR(x, format)` with Postgres's template patterns, read as
    Postgres reads them (case-sensitive, the first pattern that matches):
    `HH` / `HH12`, `HH24`, `MI`, `SS`, `MS`, `US`, `FF1` to `FF6`, `SSSS` /
    `SSSSS`, `AM` / `PM` / `A.M.` / `P.M.`, `Y,YYY`, `YYYY`, `YYY`, `YY`,
    `Y`, `IYYY`, `IYY`, `IY`, `I`, `BC` / `AD` / `B.C.` / `A.D.`, `MONTH` /
    `Month` / `month`, `MON` / `Mon` / `mon`, `MM`, `DAY` / `Day` / `day`,
    `DY` / `Dy` / `dy`, `DDD`, `IDDD`, `DD`, `D`, `ID`, `W`, `WW`, `IW`,
    `CC`, `J`, `Q`, `RM` / `rm`, `TZ` / `tz`, `TZH`, `TZM` and `OF`. The
    numbers and the meridiem and era indicators can also be written in
    lower case (`yyyy-mm-dd`, `pm`).
    - Modifiers: `FM` before a pattern (no padding), `TM` (names without
      padding; they are English), and `TH` / `th` after a number (`DDth` is
      `29th`). `FX`, and `SP` after a pattern, are accepted and ignored, as
      in Postgres
    - Names are padded to 9 characters and `RM` to 4 (`'XI  '`), numbers
      with zeros, unless `FM` is given; `MS`, `US` and `FF1` to `FF6` are
      always padded. `CC` is the century that starts in a year ending in
      01, and `-01` for the 1st century BC. Years before 1 AD are their BC
      years, for `BC` (`0044 BC`)
    - `TZ` is `UTC` for a timestamp with time zone or a date, and empty for
      a timestamp; `TZH`, `TZM` and `OF` are `+00`, `00` and `+00`. A time
      is formatted as on 1970-01-01
    - Anything else is copied: text in double quotes (where `\` escapes the
      next character), `\"` (a `"`), and every character that doesn't start
      a pattern, so `Mm` is `Mm` while `Dd` is two weekday numbers, as in
      Postgres. An empty format gives NULL
  - `TO_DATE(text, format)` and `TO_TIMESTAMP(text, format)` read the same
    templates, with the patterns `YYYY` (also `IYYY`), `YY`, `MM`, `MONTH`
    / `MON` in any case, `DD`, `HH24`, `HH12` / `HH`, `MI`, `SS`, `MS`, `US`
    and `AM` / `PM` / `A.M.` / `P.M.`; `FX` is ignored, and other patterns
    are an error (`format pattern "WW" cannot be parsed`)
  - Time zones, as in Postgres: `x AT TIME ZONE zone` (also
    `TIMEZONE(zone, x)`) reads a `TIMESTAMP` as a local time in `zone` and
    gives that instant, a `TIMESTAMP WITH TIME ZONE`, and gives the local
    time in `zone` of a `TIMESTAMP WITH TIME ZONE`, a `TIMESTAMP`. A `DATE`
    and text are read as a `TIMESTAMP WITH TIME ZONE`, and the precision
    is kept. `x AT LOCAL` (also `TIMEZONE(x)`) converts to or from the
    session time zone, UTC. There is no `TIME WITH TIME ZONE`, so a `TIME`
    is an error (`function timezone(varchar, time) does not exist`)
    - `AT TIME ZONE` binds tighter than `*` and `+` and looser than `::`
      and unary minus, left to right, so dbt_date's `cast(cast(x as
      timestamp) at time zone 'UTC' at time zone 'America/Los_Angeles' as
      timestamp)` converts from UTC to Los Angeles time
    - `CONVERT_TIMEZONE(source, target, x)` (Snowflake, Redshift; what
      dbt_date's `convert_timezone` and `now()` emit) reads `x` as a local
      time in `source` and gives the local time in `target`, a
      `TIMESTAMP`. A `TIMESTAMP WITH TIME ZONE` is read as its UTC time, as
      Snowflake reads it. `CONVERT_TIMEZONE(target, x)` reads `x` in UTC and
      also gives a `TIMESTAMP`, as in Redshift (Snowflake gives a
      `TIMESTAMP_TZ`, which has no equivalent here)
    - Zones are looked up in Postgres's order. First the time zone
      abbreviations, which are fixed offsets: `UTC`, `UT`, `UCT`, `GMT`,
      `Z`, `ZULU`, `EST` / `EDT`, `CST` / `CDT`, `MST` / `MDT`, `PST` /
      `PDT`, `AKST` / `AKDT`, `HST`, `AST` / `ADT`, `NST` / `NDT`, `WET` /
      `WEST`, `BST`, `CET` / `CEST`, `EET` / `EEST`, `JST`, `KST`, `HKT`,
      `AWST`, `ACST` / `ACDT`, `AEST` / `AEDT` and `NZST` / `NZDT` (so
      `CET` is UTC+1 in July too). Then the IANA zone names
      (`America/New_York`, `Etc/GMT+5`), case-insensitive, from the tz
      database built into the driver. Then POSIX-style offsets: `UTC+5`,
      `+05:30`, `5` or `<+0530>-05:30`, whose sign is west of Greenwich, as
      in POSIX and Postgres, so `UTC+5` and `+05:30` are behind UTC. An
      `INTERVAL` is an offset east of Greenwich, as in ISO 8601 (`INTERVAL
      '+05:30'` is ahead of UTC), without months or days (`interval time
      zone "1 day" must not include months or days`). Anything else is
      `time zone "x" not recognized`. Timestamp text with a zone name
      (`'2024-03-10 02:30 America/Los_Angeles'`) reads the zone the same way
    - Daylight saving time as in Postgres: a local time that the
      spring-forward transition skips is read with the offset before it
      (`TIMESTAMP '2024-03-10 02:30' AT TIME ZONE 'America/Los_Angeles'` is
      10:30 UTC), and one that the fall-back transition repeats with the
      offset after it (`TIMESTAMP '2024-11-03 01:30'` is 01:30 PST, 09:30
      UTC)
    - Not supported: a session time zone other than UTC, `TIME WITH TIME
      ZONE`, POSIX zones with daylight saving rules
      (`CET-1CEST,M3.5.0,M10.5.0/3`), abbreviations other than those above
      (such as `IST`, which Postgres reads as Israel's), and `TO_CHAR` of an
      interval
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
  - A result outside its type's range is an error, as in Postgres, rather
    than wrapping. A `DATE` is an Arrow `date32` (-5877641-06-23 to
    5881580-07-11): past it, `date ± integer`, `MAKE_DATE`, `DATEADD`, the
    other date functions and casts to `DATE` give "date out of range". A
    timestamp past the range of its unit (microseconds: about ±292,000 years
    from 1970) is "timestamp out of range". An interval's time part is
    nanoseconds in 64 bits (about ±2,562,047 hours); past that, and for a
    months or days count beyond 32 bits, it is "interval out of range"
  - `AGE(a, b)` / `AGE(x)` (years, months, days and time), and `EXTRACT`
    on intervals
  - Intervals are returned as Arrow `month_day_nano_interval`; Arrow
    interval and duration values can be bound and ingested
- JSON stored as text in `VARCHAR` columns (there is no JSON column type).
  Documents are parsed by the driver in each call, on the rows the index
  returns; nothing JSON-specific is pushed down. Semantics follow Postgres:
  - Operators: `doc -> 'key'` and `doc -> n` (an array element; negative `n`
    counts from the end) return JSON, `doc ->> …` returns text (a string's
    value, NULL for JSON null); `doc #> '{a,0,b}'` and `#>>` follow a path
    given as a text array. As in Postgres they share one precedence level
    with `||`, left to right, below `+` / `-` and above comparisons:
    `doc ->> 'a' || '!'` is `(doc ->> 'a') || '!'`, while `'x' || doc ->> 'a'`
    applies `->>` to the concatenation
  - `JSON_EXTRACT_PATH(doc, key, …)`, `JSON_EXTRACT_PATH_TEXT`,
    `JSON_TYPEOF`, `JSON_ARRAY_LENGTH`; `x::json` / `CAST(x AS JSON)` checks
    the text, `x::jsonb` normalizes it (both give `VARCHAR`)
  - These read the json type: a value keeps its text from the document, so
    numbers are never rounded (`'{"n": 1e400}' ->> 'n'` is `1e400`) and of
    duplicate keys the last one wins. Malformed JSON is an error,
    `invalid input syntax for type json: …` with Postgres's detail, and so
    are `\u` escapes Postgres can't decode (code point 0, unpaired
    surrogates). `doc IS JSON` guards against bad rows
  - The `JSONB_` spellings (`JSONB_EXTRACT_PATH`, `JSONB_TYPEOF`,
    `JSONB_BUILD_OBJECT`, `JSONB_AGG`, …), `::jsonb` and `RETURNING JSONB`
    give jsonb's text: keys sorted (shorter first) without duplicates,
    numbers as exact `NUMERIC` (`1e2` is `100`, `1.50` stays `1.50`), and
    `{"a": 1}` spacing
  - `x IS [NOT] JSON [VALUE | SCALAR | ARRAY | OBJECT]
    [{WITH | WITHOUT} UNIQUE [KEYS]]` is false for malformed text and NULL
    for NULL
  - SQL/JSON query functions, as in Postgres 17 (documents are read as
    jsonb): `JSON_VALUE(doc, path [RETURNING type] [behavior ON EMPTY]
    [behavior ON ERROR])` with `ERROR`, `NULL` or `DEFAULT expr`;
    `JSON_QUERY(doc, path [RETURNING type]
    [{WITHOUT | WITH [CONDITIONAL | UNCONDITIONAL]} [ARRAY] WRAPPER]
    [{KEEP | OMIT} QUOTES] …)`, which also allows `EMPTY [ARRAY]` and
    `EMPTY OBJECT`; `JSON_EXISTS(doc, path [{TRUE | FALSE | UNKNOWN | ERROR}
    ON ERROR])`. The defaults are `NULL ON EMPTY` and `NULL ON ERROR`
    (`FALSE ON ERROR` for `JSON_EXISTS`). `ON ERROR` covers a malformed
    document, a strict-mode path error, a result of the wrong shape (several
    items, or an object for `JSON_VALUE`) and a failed conversion to the
    `RETURNING` type; an invalid path is always an error
  - Paths: `[lax | strict] $` followed by `.key`, `."key"`, `.*`, `[n]`,
    `[last]`, `[last - n]`, `[a to b]`, lists like `[0, 2 to last]`, and
    `[*]`. Lax mode (the default) applies `.key` to each element of an
    array, treats a non-array as an array of one for `[n]`, and skips
    missing keys and out-of-range subscripts; strict mode makes them errors.
    Filters (`?(…)`), item methods (`.size()`, …), arithmetic, `.**` and
    `PASSING` variables are not supported
  - Constructors: `JSON_BUILD_OBJECT(k, v, …)`, `JSON_BUILD_ARRAY(…)`,
    `JSON_OBJECT(k VALUE v | k : v, … [{NULL | ABSENT} ON NULL]
    [{WITH | WITHOUT} UNIQUE [KEYS]] [RETURNING JSON | JSONB | VARCHAR])`
    (and Postgres's `JSON_OBJECT('{k,v,…}')` / `JSON_OBJECT(keys, values)`
    over text arrays), `JSON_ARRAY(v, … [{ABSENT | NULL} ON NULL]
    [RETURNING …])`, `TO_JSON(x)`. Values are encoded as Postgres's
    `to_json` does: numbers and booleans as such, NULL as `null`, strings
    escaped, dates and timestamps as ISO 8601 strings
    (`"2024-01-15T10:30:00+00:00"`), binary values as `"\\x…"`, and with
    Postgres's spacing (`{"a" : 1}`, `[1, 2]`)
  - Aggregates, computed by the driver: `JSON_AGG(x [ORDER BY …])` and
    `JSON_OBJECT_AGG(k, v [ORDER BY …])`, also with `DISTINCT` (whose values
    come out sorted). NULL values are `null`, a NULL key is an error, and no
    rows give NULL. `ORDER BY` puts NULLs last by default in either
    direction, like the driver's `ORDER BY` (Postgres puts them first for
    `DESC`). They are not available as window functions
  - Results are `VARCHAR`. An argument is embedded as JSON when it is itself
    JSON: a JSON function, `->`, `#>` or `::json`. Any other text becomes a
    JSON string, including JSON read back from a table, view or subquery, so
    write `col::json` to embed it
  - Differences from Postgres: doubles keep the driver's text form (`1e+06`
    where Postgres writes `1000000`), and the SQL/JSON functions apply
    `ON ERROR` to a malformed document too (Postgres raises the error when
    it converts the text to jsonb)
  - Not supported: the other jsonb operators (`@>`, `?`, `||` on jsonb,
    `-`), set-returning functions (`JSON_EACH`, `JSON_ARRAY_ELEMENTS`,
    `JSON_TABLE`), `JSONB_SET` and the other modification functions,
    `JSON_ARRAY(SELECT …)`, and a JSON column type (a RedisJSON-backed one
    is possible future work)
- `SELECT` without `FROM` for literal expressions
- `information_schema` (read-only, built from the driver's metadata when
  queried): `schemata`, `tables` (`BASE TABLE` / `VIEW` / `LOCAL TEMPORARY`,
  each table's row `key_prefix` and `index_name`, NULL for views, and
  `comment`), `columns` (`ordinal_position`, `column_default`, `data_type`, `is_nullable`,
  `character_maximum_length`, `character_octet_length`, `numeric_precision`,
  `numeric_scale`, `datetime_precision`, `is_indexed` and `comment`), `views`
  (`view_definition`), and `routines`, the driver's functions (see "Function
  calls" above). `comment` is the `COMMENT ON` text, NULL without
  one. Any SQL works on them, including joins
- `[WITH …] UPDATE t [[AS] a] SET col = …, … [FROM item, …] [WHERE …]` and
  `[WITH …] DELETE FROM t [[AS] a] [USING item, …] [WHERE …]` (the Postgres
  forms). The FROM / USING items are written like a SELECT's FROM clause
  (tables, views, CTEs and `(SELECT …)`, with commas or joins) and are
  joined with the target; SET and WHERE can read all of them. A target row
  that matches several FROM / USING rows is updated or deleted once. For
  UPDATE the matches must give the same new values, otherwise it's an error
  (Postgres silently uses one of them)
- `UPDATE … SET (a, b) = …` assigns a row to a column list, as in Postgres,
  also in `UPDATE … FROM` and in MERGE's `UPDATE SET`. The row is
  `(x, y, …)`, `ROW(x, …)` (also for a single column) or a subquery
  `(SELECT x, y …)`, which must return as many columns ("number of columns
  does not match number of values") and at most one row: no row sets the
  columns to NULL, more than one is an error. The subquery runs once per
  row (once in all if it isn't correlated), not once per column. As with
  `col = …`, every value is computed from the row before the change, so
  `SET (a, b) = (b, a)` swaps
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
  deleted. `INSERT` gives the columns it leaves out (or sets to `DEFAULT`)
  their defaults, and `INSERT DEFAULT VALUES` inserts a row of defaults
- `RETURNING …` at the end of `INSERT`, `UPDATE`, `DELETE` and `MERGE`,
  written like a select list: `*`, `t.*`, and expressions with optional
  aliases, including subqueries (aggregates and window functions are not
  allowed, as in Postgres). It reads the target's columns, and in
  `UPDATE … FROM`, `DELETE … USING` and `MERGE` the other items' too. As in
  Postgres, `*` lists the target's columns first, then the FROM / USING
  items'; in `MERGE`, the source's columns, then the target's. The
  statement returns one row per row it changed (`INSERT`: in `VALUES` or
  query order; otherwise in no particular order):
  - `INSERT`: the new row as stored (cast to the column types);
    `RETURNING __rowid` gives the row id it was given
  - `UPDATE`: the row after the change
  - `DELETE`: the deleted row
  - `MERGE`: each row inserted, updated or deleted, with the source row it
    came from (NULLs for `WHEN NOT MATCHED BY SOURCE`); `merge_action()`
    (Postgres 17) is `'INSERT'`, `'UPDATE'` or `'DELETE'`

  The list is computed with the other checks, before anything is written, so
  an error in it leaves the table untouched, and its subqueries see the
  tables as they were before the statement. `ExecuteQuery` returns the rows
  (with the RETURNING columns, also when no row changed); `ExecuteUpdate`
  runs the statement and returns the number of rows changed, as Postgres
  drivers do; `ExecuteSchema` returns the columns without running it. Without
  `RETURNING`, `ExecuteQuery` on these statements still returns an empty
  result with no columns, and the number of rows changed. `__rowid` can't be
  read in the `RETURNING` list of `UPDATE … FROM`, `DELETE … USING` or
  `MERGE` (as in any join), and `merge_action()` can't be called inside a
  subquery
- Transactions and settings. The driver is autocommit only: every statement
  commits on its own. SQL written for Postgres still sends transaction
  control and `SET` (dbt sends a literal `commit;` before hooks that run
  outside the transaction, and `sql_header` often holds `SET`s), so they are
  accepted as Postgres accepts them in autocommit mode:
  - **Transaction control does nothing:** `BEGIN [WORK | TRANSACTION]
    [mode, …]`, `START TRANSACTION [mode, …]`, `COMMIT [WORK | TRANSACTION]
    [AND [NO] CHAIN]`, `END …`, `ROLLBACK [WORK | TRANSACTION] [AND [NO]
    CHAIN]` and `ABORT …`, and also `SET TRANSACTION mode, …` and `SET
    SESSION CHARACTERISTICS AS TRANSACTION mode, …`. The modes
    (`ISOLATION LEVEL …`, `READ ONLY`, `READ WRITE`, `[NOT] DEFERRABLE`) are
    accepted and ignored, so `READ ONLY` doesn't stop writes. **`ROLLBACK`
    undoes nothing**: in `BEGIN; INSERT …; ROLLBACK` the row stays, because
    the `INSERT` committed. Postgres warns when there is no transaction to
    commit or roll back; ADBC has no way to return warnings, so nothing is
    reported. `COMMIT AND CHAIN` outside `BEGIN` is Postgres's error
    `COMMIT AND CHAIN can only be used in transaction blocks`.
  - **Not supported:** `SAVEPOINT`, `RELEASE [SAVEPOINT]`, `ROLLBACK TO
    [SAVEPOINT]`, `COMMIT PREPARED` / `ROLLBACK PREPARED` and `SET
    TRANSACTION SNAPSHOT` (`SAVEPOINT is not supported (autocommit only)`, …).
    The error comes when the script is parsed, so none of its statements
    run.
  - **`SET [SESSION | LOCAL] name {= | TO} {value [, …] | DEFAULT}`,
    `RESET name`, `RESET ALL`, `SHOW name` and `SHOW ALL`**, also `SET TIME
    ZONE …`, `RESET TIME ZONE`, `SHOW TIME ZONE`, `SET SCHEMA 'name'`
    (`search_path`) and `SET NAMES 'name'` (`client_encoding`), for the
    parameters below. Names are case-insensitive. Other names, Postgres's
    custom `a.b` ones included, give `unrecognized configuration parameter
    "x"`. `SHOW` returns one `VARCHAR` column named as in Postgres
    (`DateStyle`, `TimeZone`, …); `SHOW ALL` returns `name`, `setting` and
    `description`. Settings belong to the connection and last until it
    closes; `ROLLBACK` doesn't undo them either.
  - **`SET LOCAL`** lasts until the transaction ends, as in Postgres: until
    `COMMIT`, `END`, `ROLLBACK` or `ABORT` after a `BEGIN` (the connection
    remembers that a `BEGIN` is open, for this only), or else until the end
    of the script it is in (Postgres runs a script of several statements as
    one transaction). A `SET LOCAL` statement on its own, outside `BEGIN`,
    is checked and has no effect, as in Postgres.
  - **Parameters:**

    | Parameter | Values | Default |
    |-|-|-|
    | `search_path` | Schemas, comma-separated. Unqualified names use the first one, skipping `"$user"`, `pg_catalog` and `pg_temp` (temporary objects are always found first), as `adbc.redis.default_schema` sets it: `CREATE`, queries, writes and bulk ingest. The schema needn't exist yet (schemas exist once they have a table). `SHOW` shows the list as set, and the ADBC current schema (`adbc.connection.db_schema`) is the same setting | `adbc.redis.default_schema` |
    | `TimeZone` (`SET TIME ZONE`) | Only `UTC`, `LOCAL` and `DEFAULT`, which are UTC: the session time zone is always UTC (see the date/time functions). Others give `invalid value for parameter "TimeZone": "…" (only UTC is supported)` | `UTC` |
    | `client_encoding` | Only `UTF8` (`UTF-8`, `UNICODE`) | `UTF8` |
    | `application_name` | Any; not used | `''` |
    | `standard_conforming_strings` | Only `on`: `'…'` strings don't treat backslashes as escapes | `on` |
    | `statement_timeout`, `lock_timeout`, `idle_in_transaction_session_timeout` | Milliseconds, or a number with a unit (`us`, `ms`, `s`, `min`, `h`, `d`), shown as Postgres shows them (`5s`, `1500ms`). Not enforced: there is no statement timeout (and there are no locks or transactions) | `0` |
    | `extra_float_digits` | -15 to 3; no effect, as results are Arrow values, not text | `1` |
    | `DateStyle` | Only the `ISO` style, with an order `MDY`, `DMY` or `YMD`, which has no effect (only ISO dates are read) | `ISO, MDY` |
    | `IntervalStyle` | Only `postgres`, the format of intervals cast to text | `postgres` |

    The same settings through ADBC options (other than the current schema)
    are not supported.
- Not supported: mutually recursive CTEs, `ANY` / `ALL` over arrays or
  value lists, set-returning functions in the SELECT list
  (`SELECT generate_series(1, 3)`), table functions other than
  `GENERATE_SERIES`, data-modifying statements in `WITH` (`WITH d AS
  (DELETE … RETURNING …) INSERT …`), `RETURNING OLD.* / NEW.*` (Postgres
  18), `GROUP BY DISTINCT` (which drops repeated grouping sets),
  `GROUPING()` of an enclosing query's columns inside a subquery,
  `PERCENTILE_CONT` / `PERCENTILE_DISC` of an array of fractions (there is
  no array type), and the ordered-set aggregates and `MEDIAN` as window
  functions
- Types: `BOOLEAN, SMALLINT, INTEGER, BIGINT, REAL, DOUBLE PRECISION,
  NUMERIC(p,s), VARCHAR(n), CHAR(n), VARCHAR/TEXT, VARBINARY/BLOB, DATE,
  TIME(p), TIMESTAMP(p) [WITH TIME ZONE], INTERVAL` (interval columns are
  stored but not indexed; see String lengths for `VARCHAR(n)` / `CHAR(n)`).
  `TIME WITH TIME ZONE` is a `TIME`: there is no time-of-day type with a
  zone.
  JSON documents are stored in `VARCHAR` columns (`NOINDEX` if they are
  never compared as a whole); `JSON` and `JSONB` are only cast targets

Tables can be qualified as `schema.table` or `redis.schema.table`, and
`pg_temp.table` is the connection's temporary table. Strings are written
`'…'` (`''` for a quote) or dollar-quoted, `$$…$$` or `$tag$…$tag$`, as in
Postgres. Schemas are key
namespaces (default `public`; `SET search_path` changes it for the
connection). There are no transactions (autocommit only; `BEGIN`, `COMMIT`
and `ROLLBACK` are accepted and do nothing, see "Transactions and settings"
above). `UPDATE`, `DELETE` and `MERGE` find their rows and compute and check
every change first (new values and casts, string lengths, `NOT NULL`, `CHECK`, MERGE's
one-change-per-row rule, `RETURNING`), so such an error leaves the table untouched. Then
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
| `adbc.redis.default_schema` | database | Schema for unqualified names (default `public`). `SET search_path` and the ADBC current schema change it for one connection; `RESET search_path` goes back to it |
| `adbc.redis.aggregate_pushdown` | database, statement | `exact` / `all` / `none` |
| `adbc.redis.rename_rekey` | database, connection | `false` (default): `RENAME TO` only changes metadata. `true`: it also moves the rows and index to the new name's keys, in time proportional to the number of rows, and the table refuses changes meanwhile (see `ALTER TABLE`). The connection option overrides the database's |
| `adbc.redis.read_timeout` | database, connection | How long the client waits for each reply: `30s`, `10m`, … or a number of seconds; `0` for no timeout. Default `5m`, or the URI's `read_timeout` (see **Timeouts** under [Server requirements](#server-requirements)). The connection option overrides the database's |
| `adbc.redis.write_timeout` | database, connection | How long the client waits to send each command, in the same format. Default: the read timeout, or the URI's `write_timeout` |
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
| `COUNT(*)` with `LIKE 'gi%'` (index prefix query, profiled first) | 1 | 62 | 64 | 5 |
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
  correlated scalar subquery costs one indexed query per distinct outer
  value (about 0.6 ms each here).
- **`IN (SELECT …)` and correlated `EXISTS` / `IN` on equalities don't
  run per row** (`bench.py` has three such queries over the 100,000 rows).
  `order_id IN (SELECT …)` with 50,000 values takes 0.74 s: each row is
  checked against a hash set of the values. `EXISTS (SELECT … WHERE
  r.order_id = s.order_id AND …)` with 50,000 inner rows takes 0.87 s: it
  runs per outer row for the first 500 rows, then reads the inner side once
  as a hash semi-join. When the subquery has no rows it takes 2 ms, because
  the index then rules out every row. Without the hash set, each row was
  compared with every value (23 s), and without the semi-join the subquery
  ran once per outer row (100,000 `FT.AGGREGATE` commands, 22–23 s).
- **dbt's `delete+insert` with a composite key** (`where (k1, k2) in
  (select distinct k1, k2 from …)`) deleted 10,000 keys from a table of
  200,000 rows in about 1 s, standalone and on a 3-shard cluster, and
  inserted them in 0.4 and 0.15 s (measured separately with
  `TestSQLRowValueScale`: Redis 8.6.2 on a machine shared with other work,
  median of 4 runs, which ranged from 0.8 to 2.2 s). Every row is read and
  checked against a hash
  set of the keys. The per-key forms, `DELETE … USING` and `EXISTS`, also
  read every row and took 1.2–1.6 s. When a key column of the keys has at
  most 1,000 distinct values, the index fetches only the rows with those
  values, and the delete took 0.2–0.3 s.
- **Queries that can't return rows read nothing**, only the tables'
  metadata. On a table of 152,686 rows (measured separately, best of 3,
  standalone), dbt's `select * from (<model>) as __dbt_sbq where false
  limit 0` over a model with two `GROUP BY`s and a join takes 1.2 ms, and
  the model's `dbt run --empty` form (each ref read as `(select * from t
  where false limit 0)`) 1.0 ms. Up to v0.0.6 they ran in full, in 1.0 s
  (as long as the model itself) and 2.7 s; `select * from t where false
  limit 0` took 1.4 s, and now 0.5 ms.

Known ways to make the slow cases faster (not done yet):

- `COUNT(*)` with a `LIKE` prefix, and `COUNT(*)` on a lazy view, still read
  the matched rows to re-check them, although the index has already matched
  them exactly; the index could answer them alone.
- A row ordering comparison, as in keyset pagination (`WHERE (ts, id) > (?,
  ?)`), is checked on every row; the index could take the bound its first
  item implies (`ts >= ?`).
- A literal list of rows, `(a, b) IN ((1, 'x'), …)`, is checked row by row
  of the list (after the index unions); a long one could use a hash set, as
  `IN (SELECT …)` does.

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

`TestSQLRowValueScale` times dbt's `delete+insert` with a composite key
(with `-v`) on 20,000 rows; `REDIS_ROW_VALUES_ROWS=200000` runs it on
200,000 (a multiple of 200), as measured under Performance.

Some tests need more:

- **The ACL tests** (`TestACL…`) create ACL users `it_acl_*` and delete them
  afterwards, so the user needs the `ACL` command. To run the whole suite as
  a user with only the driver's commands (see **ACL users** under
  [Server requirements](#server-requirements)), set `REDIS_URI` to that user
  and `REDIS_ADMIN_URI` to one that can do everything: the tests' own checks
  use it (`KEYS`, `ACL`, …).
- **`TestTimeoutServerPaused`** pauses the server (`CLIENT PAUSE`, on one
  shard of a cluster) for about half a minute, so it only runs with
  `REDIS_PAUSE_TESTS=1`. Run it against a test server only.

To test against a local 3-shard Redis Cluster (from `go`):

```bash
docker compose --profile cluster up --detach --wait redis-cluster
```

```bash
cd validation && REDIS_URI=redis://localhost:7001/0 uv run pytest -v tests/
```

To test against Redis Flex, `flex-setup.sh` (from `go`) starts a
single-node Redis Software cluster with flash storage in Docker. It then
creates a Flex database with Search on it, and prints the database's URI.
The cluster uses about 1.5 GB of memory and the image's trial license. Its
"flash" is a directory inside the container, and its admin API (port 9443)
listens on localhost only, with fixed test credentials.

```bash
./flex-setup.sh
```

The driver refuses Flex databases (see
[Server requirements](#server-requirements)), and `TestFlexRefused` checks
that it does so without writing to the database. The test is skipped unless
`REDIS_FLEX_URI` is set:

```bash
REDIS_FLEX_URI=redis://localhost:12000/0 go test -run TestFlex ./...
```

Remove the cluster with `docker compose --profile flex down redis-flex`.
Name the service: without it, `down` also removes the `redis` container.

To build the Linux library (on Linux or a Mac, with Docker), run
`build-linux.sh` from `go` with `amd64` or `arm64`. It writes
`build/linux-<arch>/libadbc_driver_redis.so`. A foreign architecture runs
under emulation and is much slower.

```bash
./build-linux.sh arm64
```

The script builds in an AlmaLinux 8 container, against glibc 2.28: the
oldest glibc among the platforms Redis Software supports. So one library
loads on all of them, and the script fails if it would need a newer glibc.

Release binaries come from `.github/workflows/release.yml`. When a release is
published, it builds the Linux (amd64, arm64) and macOS (arm64) libraries,
runs the Go tests and the validation suite against Redis on both Linux
builds (standalone and a 3-shard cluster), and then runs the validation suite
on each Linux tarball under Ubuntu 20.04, 22.04 and 24.04, AlmaLinux 8 and 9
(RHEL 8 and 9), and Amazon Linux 2023. Only then does it attach the tarballs
and their `.sha256` files to the release. GitHub's macOS runners can't run the
Redis containers, so the macOS build is only checked to load and to pass the
Go unit tests. To rebuild an existing release's assets, run the workflow
by hand with its tag:

```bash
gh workflow run release.yml -f tag=v0.0.4
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
