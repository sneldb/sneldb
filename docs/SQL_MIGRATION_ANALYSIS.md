# SQL Migration Analysis: Replacing QUERY Command with SQL

## Executive Summary

Replacing the `QUERY` command with a SQL subset is **feasible but non-trivial**. The current query system has several domain-specific features that would need careful mapping to SQL, particularly:

1. **Event sequences** (FOLLOWED BY/PRECEDED BY) - requires temporal JOINs
2. **Context-based filtering** (FOR clause) - sneldb-specific concept
3. **Time field selection** (USING clause) - allows custom time fields
4. **Time bucketing** (PER clause) - needs DATE_TRUNC or equivalent

## Current Query Capabilities

### Basic Query Features

- **Event type selection**: Single event type (acts like table name)
- **Context filtering**: `FOR <context_id>` (sneldb-specific)
- **Time filtering**: `SINCE <timestamp>` with optional `USING <time_field>`
- **Field selection**: `RETURN [field1, field2, ...]`
- **Predicates**: `WHERE` with `=`, `!=`, `>`, `>=`, `<`, `<=`, `AND`, `OR`, `NOT`, `IN`
- **Pagination**: `LIMIT` and `OFFSET`
- **Sorting**: `ORDER BY field [ASC|DESC]`

### Aggregation Features

- **Aggregations**: `COUNT`, `COUNT UNIQUE <field>`, `COUNT <field>`, `TOTAL <field>`, `AVG <field>`, `MIN <field>`, `MAX <field>`
- **Grouping**: `BY field1, field2, ...`
- **Time bucketing**: `PER HOUR|DAY|WEEK|MONTH|YEAR [USING <time_field>]`

### Sequence Query Features

- **Event sequences**: `event_a FOLLOWED BY event_b [FOLLOWED BY event_c ...] LINKED BY field`
- **Temporal ordering**: Events must occur in specific time order
- **Cross-event filtering**: WHERE clauses can reference multiple event types
- **JOIN-like behavior**: Returns all events from matched sequences (like INNER JOIN)
  - `FOLLOWED BY` = temporal JOIN where `a.timestamp <= b.timestamp`
  - `PRECEDED BY` = temporal JOIN where `b.timestamp < a.timestamp`
  - Both events must exist (INNER JOIN semantics, not LEFT/RIGHT/FULL OUTER)
- **Multi-event sequences**: Syntax supports multiple FOLLOWED BY clauses (e.g., `A FOLLOWED BY B FOLLOWED BY C`)
  - ⚠️ **Current limitation**: Execution currently only supports 2 event types (single link)
  - Multi-link sequences are parsed but return empty results (planned for Phase 4)

## Event Sequences as JOINs

**Yes, event sequences can mimic JOIN behavior!** They return both events from matched sequences, which is equivalent to an INNER JOIN with temporal ordering:

| Sneldb Event Sequence                                              | SQL Equivalent                                                                                                                          | Status                                 |
| ------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------- |
| `QUERY page_view FOLLOWED BY order_created LINKED BY user_id`      | `SELECT a.*, b.* FROM page_view a INNER JOIN order_created b ON a.user_id = b.user_id WHERE a.timestamp <= b.timestamp`                 | ✅ Supported                           |
| `QUERY order_created PRECEDED BY payment_failed LINKED BY user_id` | `SELECT a.*, b.* FROM order_created a INNER JOIN payment_failed b ON a.user_id = b.user_id WHERE b.timestamp < a.timestamp`             | ✅ Supported                           |
| `QUERY A FOLLOWED BY B FOLLOWED BY C LINKED BY user_id`            | `SELECT a.*, b.*, c.* FROM A a JOIN B b ON a.user_id = b.user_id JOIN C c ON b.user_id = c.user_id WHERE a.ts <= b.ts AND b.ts <= c.ts` | ⚠️ Syntax supported, execution pending |

**Key points:**

- ✅ Returns all events from matched sequences (JOIN-like behavior)
- ✅ Uses equality on link field (`ON a.field = b.field`)
- ✅ Adds temporal ordering (`WHERE a.timestamp <= b.timestamp`)
- ✅ Only INNER JOIN semantics (all events must exist)
- ✅ **Syntax supports multiple event types** (e.g., `A FOLLOWED BY B FOLLOWED BY C`)
- ⚠️ **Current execution limitation**: Only 2 event types supported (single link)
- ❌ No LEFT/RIGHT/FULL OUTER JOINs
- ❌ No arbitrary JOIN conditions (only equality)

## SQL Subset Required

### Core SQL Features Needed

```sql
-- Basic SELECT
SELECT [field1, field2, ...] FROM event_type
WHERE conditions
LIMIT n OFFSET m
ORDER BY field [ASC|DESC]

-- Aggregations
SELECT COUNT(*), SUM(field), AVG(field), MIN(field), MAX(field)
FROM event_type
WHERE conditions
GROUP BY field1, field2

-- Time bucketing (PostgreSQL-style)
SELECT DATE_TRUNC('day', timestamp_field) AS bucket, COUNT(*)
FROM event_type
WHERE conditions
GROUP BY bucket

-- JOINs for sequences (complex - see below)
SELECT a.*, b.*
FROM event_a a
JOIN event_b b ON a.link_field = b.link_field
WHERE a.timestamp <= b.timestamp  -- FOLLOWED BY
```

### Sneldb-Specific Mappings

| Sneldb Feature              | SQL Equivalent                                                 | Notes                   |
| --------------------------- | -------------------------------------------------------------- | ----------------------- |
| `QUERY event_type`          | `SELECT * FROM event_type`                                     | Event type = table name |
| `FOR context_id`            | `WHERE context_id = '...'`                                     | Special column          |
| `SINCE timestamp`           | `WHERE timestamp >= ...`                                       | Needs USING support     |
| `USING time_field`          | `WHERE time_field >= ...`                                      | Custom time field       |
| `RETURN [fields]`           | `SELECT fields`                                                | Standard SQL            |
| `PER DAY USING created_at`  | `DATE_TRUNC('day', created_at)`                                | Time bucketing          |
| `BY field`                  | `GROUP BY field`                                               | Standard SQL            |
| `COUNT UNIQUE field`        | `COUNT(DISTINCT field)`                                        | Standard SQL            |
| `TOTAL field`               | `SUM(field)`                                                   | Standard SQL            |
| `FOLLOWED BY ... LINKED BY` | `INNER JOIN ... ON link_field = link_field WHERE a.ts <= b.ts` | Mimics JOIN behavior    |

## Implementation Requirements

### 1. SQL Parser Library

**Recommended**: `sqlparser-rs` (most mature Rust SQL parser)

```toml
[dependencies]
sqlparser = "0.40"
```

**Alternative**: `sqlparser` crate (different from sqlparser-rs, less maintained)

### 2. Translation Layer Architecture

```
SQL Query String
    ↓
SQL Parser (sqlparser-rs)
    ↓
SQL AST (sqlparser AST)
    ↓
Translation Layer (new module)
    ↓
Command::Query (existing type)
    ↓
Existing Query Execution Engine
```

### 3. Key Translation Challenges

#### Challenge 1: Event Type Mapping

- **Current**: `QUERY order_created` → event type is explicit
- **SQL**: `SELECT * FROM order_created` → table name = event type
- **Solution**: Map table names to event types (1:1 mapping)

#### Challenge 2: Context Filtering

- **Current**: `FOR context_id` is a top-level clause
- **SQL**: `WHERE context_id = '...'` (standard WHERE clause)
- **Solution**: Extract `context_id` conditions from WHERE clause and treat specially

#### Challenge 3: Time Field Selection

- **Current**: `SINCE timestamp USING created_at` allows custom time fields
- **SQL**: Need to detect which time field is used in WHERE clauses
- **Solution**: Parse WHERE clauses for timestamp comparisons, extract field name

#### Challenge 4: Time Bucketing

- **Current**: `PER DAY USING created_at`
- **SQL**: `DATE_TRUNC('day', created_at) AS bucket ... GROUP BY bucket`
- **Solution**: Detect DATE_TRUNC in SELECT/GROUP BY, extract granularity and field

#### Challenge 5: Event Sequences (Most Complex)

- **Current**: `QUERY page_view FOLLOWED BY order_created LINKED BY user_id`
  - Returns both events from matched sequences (JOIN-like behavior)
  - Uses temporal ordering (FOLLOWED BY = `a.timestamp <= b.timestamp`)
  - Only supports INNER JOIN semantics (both events must exist)
- **SQL**: Requires temporal JOIN:
  ```sql
  SELECT a.*, b.*
  FROM page_view a
  INNER JOIN order_created b ON a.user_id = b.user_id
  WHERE a.timestamp <= b.timestamp
  ```
- **Solution**:
  - Detect JOINs between event types (table names = event types)
  - Extract link field from JOIN condition (`ON a.field = b.field`)
  - Extract temporal ordering from WHERE clause (`a.timestamp <= b.timestamp` → FOLLOWED BY)
  - Map to `EventSequence` with `SequenceLink::FollowedBy` or `PrecededBy`
  - **Limitation**: Only INNER JOINs supported (LEFT/RIGHT/FULL OUTER not supported)

#### Challenge 6: COUNT UNIQUE

- **Current**: `COUNT UNIQUE field`
- **SQL**: `COUNT(DISTINCT field)`
- **Solution**: Detect `COUNT(DISTINCT ...)` and map to `AggSpec::Count { unique_field: Some(...) }`

### 4. Implementation Steps

#### Phase 1: Basic SELECT (No Aggregations, No Sequences)

1. Add `sqlparser` dependency
2. Create `src/command/parser/sql.rs` module
3. Parse basic SQL: `SELECT * FROM event_type WHERE ... LIMIT n`
4. Translate to `Command::Query`
5. Test with simple queries

#### Phase 2: Aggregations

1. Parse `SELECT COUNT(*), SUM(field) FROM ... GROUP BY ...`
2. Translate aggregations to `AggSpec`
3. Handle `COUNT(DISTINCT field)` → `COUNT UNIQUE field`
4. Test aggregation queries

#### Phase 3: Time Bucketing

1. Parse `DATE_TRUNC('day', field)` in SELECT/GROUP BY
2. Extract granularity and field
3. Translate to `time_bucket` and `using_field`
4. Test time-bucketed queries

#### Phase 4: Context and Time Field Detection

1. Extract `context_id` conditions from WHERE clause
2. Detect timestamp comparisons to identify time field
3. Translate to `context_id` and `since`/`using_field`
4. Test context filtering and custom time fields

#### Phase 5: Event Sequences (Most Complex)

1. Parse JOINs between event types
2. Extract link field from JOIN condition
3. Detect temporal ordering in WHERE clause
4. Translate to `EventSequence` with `SequenceLink`
5. Handle event-prefixed fields in WHERE clauses
6. Test sequence queries

#### Phase 6: Edge Cases and Polish

1. Handle all WHERE clause operators
2. Support `RETURN [fields]` → `SELECT fields`
3. Support `ORDER BY`
4. Support `OFFSET`
5. Comprehensive testing

## Code Structure

### New Files Needed

```
src/command/parser/
  ├── sql.rs              # SQL parser entry point
  ├── sql_translator.rs   # SQL AST → Command::Query translator
  └── sql_utils.rs        # Helper functions for SQL parsing

src/command/parser/commands/
  └── query.rs            # Modify to support SQL parsing (or keep separate)
```

### Modified Files

```
src/command/parser/command.rs
  - Add SQL parsing branch (detect SELECT keyword)
  - Route to sql parser

src/command/types.rs
  - No changes needed (Command::Query already supports all features)
```

## Example Translations

### Example 1: Basic Query

**Sneldb:**

```sneldb
QUERY order_created WHERE status="confirmed" LIMIT 100
```

**SQL:**

```sql
SELECT * FROM order_created WHERE status = 'confirmed' LIMIT 100
```

### Example 2: Aggregation

**Sneldb:**

```sneldb
QUERY orders COUNT, TOTAL amount, AVG amount BY country
```

**SQL:**

```sql
SELECT country, COUNT(*), SUM(amount) AS total_amount, AVG(amount) AS avg_amount
FROM orders
GROUP BY country
```

### Example 3: Time Bucketing

**Sneldb:**

```sneldb
QUERY orders TOTAL amount PER DAY USING created_at
```

**SQL:**

```sql
SELECT DATE_TRUNC('day', created_at) AS bucket, SUM(amount) AS total_amount
FROM orders
GROUP BY bucket
ORDER BY bucket
```

### Example 4: Context Filtering

**Sneldb:**

```sneldb
QUERY order_created FOR "user_123" SINCE "2025-01-01T00:00:00Z"
```

**SQL:**

```sql
SELECT * FROM order_created
WHERE context_id = 'user_123' AND timestamp >= '2025-01-01T00:00:00Z'
```

### Example 5: Event Sequence (Complex)

**Sneldb:**

```sneldb
QUERY page_view FOLLOWED BY order_created LINKED BY user_id
WHERE page_view.page="/checkout"
```

**SQL:**

```sql
SELECT a.*, b.*
FROM page_view a
JOIN order_created b ON a.user_id = b.user_id
WHERE a.timestamp <= b.timestamp AND a.page = '/checkout'
```

## Limitations and Considerations

### 1. Sneldb-Specific Features

- **Context ID**: Must be extracted from WHERE clause (not a SQL standard)
- **Time field selection**: SQL doesn't have `USING` clause - must infer from WHERE
- **Event sequences**: Can mimic JOIN behavior but with limitations:
  - ✅ Supports INNER JOIN semantics (all events must exist)
  - ✅ Supports temporal ordering (FOLLOWED BY/PRECEDED BY)
  - ✅ **Syntax supports multiple event types** (e.g., `A FOLLOWED BY B FOLLOWED BY C`)
  - ⚠️ **Current execution limitation**: Only 2 event types supported (single link) - multi-link sequences parsed but not executed
  - ❌ Does NOT support LEFT/RIGHT/FULL OUTER JOINs
  - ❌ Does NOT support arbitrary JOIN conditions (only equality on link field)

### 2. SQL Dialect Differences

- **DATE_TRUNC**: PostgreSQL syntax, may need adaptation for other dialects
- **String literals**: SQL uses single quotes, sneldb uses double quotes
- **Field names**: SQL may require quoting for special characters

### 3. Backward Compatibility

- **Option A**: Support both SQL and current syntax (recommended)
- **Option B**: Replace current syntax entirely (breaking change)
- **Option C**: Add SQL as alternative syntax (`QUERY SQL "SELECT ..."`)

### 4. Error Handling

- SQL parsing errors need to be translated to sneldb error format
- Unsupported SQL features need clear error messages
- Ambiguous queries (e.g., multiple time fields) need validation

## Estimated Effort

- **Phase 1 (Basic SELECT)**: 1-2 weeks
- **Phase 2 (Aggregations)**: 1 week
- **Phase 3 (Time Bucketing)**: 1 week
- **Phase 4 (Context/Time Field)**: 1 week
- **Phase 5 (Event Sequences)**: 2-3 weeks (most complex)
- **Phase 6 (Polish)**: 1-2 weeks

**Total**: ~8-12 weeks for full implementation

## Recommendations

1. **Start with Phase 1**: Implement basic SELECT queries first
2. **Keep both syntaxes**: Support SQL alongside current syntax for backward compatibility
3. **Use sqlparser-rs**: Most mature and actively maintained
4. **Incremental approach**: Implement phases sequentially, test thoroughly
5. **Consider SQLite dialect**: sqlparser-rs supports multiple dialects, SQLite is simplest
6. **Document limitations**: Clearly document which SQL features are supported

## Alternative Approach: SQL-Like Syntax

Instead of full SQL parsing, consider a **SQL-like syntax** that's easier to parse:

```sql
-- Sneldb SQL-like syntax
SELECT * FROM order_created WHERE status = 'confirmed' LIMIT 100
SELECT COUNT(*), SUM(amount) FROM orders GROUP BY country
SELECT DATE_TRUNC('day', created_at), SUM(amount) FROM orders GROUP BY 1
```

This would be:

- Easier to implement (custom parser)
- More predictable (no SQL dialect differences)
- Still familiar to SQL users
- Can map directly to existing Command::Query structure

## Conclusion

Replacing the QUERY command with SQL is **feasible** but requires:

1. SQL parser library (sqlparser-rs recommended)
2. Translation layer (SQL AST → Command::Query)
3. Careful handling of sneldb-specific features
4. Incremental implementation approach
5. Consideration of backward compatibility

The most complex part is **event sequences** (FOLLOWED BY/PRECEDED BY), which **can mimic JOIN behavior** but with limitations:

- ✅ They return all events from matched sequences (like INNER JOIN)
- ✅ They support temporal ordering (FOLLOWED BY = `a.timestamp <= b.timestamp`)
- ✅ **Syntax supports multiple event types** (e.g., `A FOLLOWED BY B FOLLOWED BY C`)
- ⚠️ **Current execution limitation**: Only 2 event types supported (single link) - multi-link sequences are parsed but return empty results
- ❌ They only support INNER JOIN semantics (no LEFT/RIGHT/FULL OUTER JOINs)
- ❌ They're limited to equality joins on a single link field

So simple SQL JOINs can be translated to event sequences, and multi-table JOINs could be supported once multi-link execution is implemented.

**See also**: [Multi-Link Sequence Implementation Analysis](./MULTI_LINK_SEQUENCE_IMPLEMENTATION.md) for detailed breakdown of changes needed to support multi-link sequences (JOINs).
