# Filter Architecture and Zone Collection

## Overview

SnelDB's filter system efficiently prunes zones (fixed-size data blocks) before reading column data, dramatically reducing I/O for complex queries. The architecture transforms user queries into a logical filter tree, collects candidate zones for each filter, and combines them using set operations (intersection for AND, union for OR) to determine which zones to scan.

## Core Concepts

- **FilterGroup**: A tree structure representing the logical structure of WHERE clauses (AND, OR, NOT, individual filters)
- **Zone Collection**: The process of identifying candidate zones that might contain matching data
- **Zone Combination**: Set operations (intersection/union) to combine zones from multiple filters
- **Index Strategy**: How a filter is applied (ZoneXorIndex for equality, ZoneSuRF for ranges, FullScan as fallback)

## FilterGroup Structure

The `FilterGroup` enum preserves the logical structure from WHERE clauses:

```rust
enum FilterGroup {
    Filter { column, operation, value, ... },
    And(Vec<FilterGroup>),
    Or(Vec<FilterGroup>),
    Not(Box<FilterGroup>),
}
```

**Example**: The query `WHERE (status = "active" OR status = "pending") AND priority > 4` becomes:

```
And([
    Or([
        Filter { column: "status", operation: Eq, value: "active" },
        Filter { column: "status", operation: Eq, value: "pending" }
    ]),
    Filter { column: "priority", operation: Gt, value: 4 }
])
```

## Query Transformation Pipeline

### 1. Expression Parsing

The PEG parser converts the WHERE clause into an `Expr` tree:

```sneldb
QUERY orders WHERE id IN (1, 2, 3) AND status = "active"
```

Becomes:

```
And(
    In { field: "id", values: [1, 2, 3] },
    Compare { field: "status", op: Eq, value: "active" }
)
```

### 2. FilterGroup Building

`FilterGroupBuilder` transforms `Expr` → `FilterGroup` with optimizations:

#### IN Operator Expansion

`IN` operators are expanded into `OR` of equality filters for efficient zone collection:

```rust
// id IN (1, 2, 3) becomes:
Or([
    Filter { column: "id", operation: Eq, value: 1 },
    Filter { column: "id", operation: Eq, value: 2 },
    Filter { column: "id", operation: Eq, value: 3 }
])
```

**Why**: Each equality can use `ZoneXorIndex` for fast zone lookup, then zones are unioned.

#### OR Equality Expansion

Multiple equality comparisons on the same field are automatically expanded:

```rust
// status = "active" OR status = "pending" becomes:
Or([
    Filter { column: "status", operation: Eq, value: "active" },
    Filter { column: "status", operation: Eq, value: "pending" }
])
```

**Why**: Same optimization as IN—each equality uses an index, then union.

#### OR Flattening

Nested OR structures are flattened to avoid unnecessary tree depth:

```rust
// OR(A, OR(B, C)) becomes OR(A, B, C)
```

**Why**: Simplifies zone combination logic and improves performance.

### 3. Zone Collection

`ZoneCollector` orchestrates zone collection:

1. **Extract unique filters**: Deduplicate filters to avoid redundant zone lookups
2. **Build zone cache**: For each unique filter, collect candidate zones from all segments
3. **Combine zones**: Use `ZoneGroupCollector` to traverse the FilterGroup tree and combine zones

**Example**: For `(status = "active" OR status = "pending") AND priority > 4`:

- Collect zones for `status = "active"` → `[zone_1, zone_3]`
- Collect zones for `status = "pending"` → `[zone_2, zone_4]`
- Collect zones for `priority > 4` → `[zone_2, zone_3, zone_5]`
- Combine: `OR([zone_1, zone_3], [zone_2, zone_4])` = `[zone_1, zone_2, zone_3, zone_4]`
- Then: `AND([zone_1, zone_2, zone_3, zone_4], [zone_2, zone_3, zone_5])` = `[zone_2, zone_3]`

## Smart NOT Handling

NOT operations require special handling because "NOT matching" means "all zones except matching zones."

### NOT(Filter)

For a single filter, compute the complement:

1. Get all zones for all segments in the query
2. Collect zones matching the filter
3. Return: `all_zones - matching_zones`

**Example**: `NOT status = "active"` returns all zones except those containing `status = "active"`.

### NOT(AND) - De Morgan's Law

Transform using De Morgan's law: `NOT(A AND B)` = `NOT A OR NOT B`

```rust
// NOT(status = "active" AND priority > 4) becomes:
Or([
    Not(Filter { status = "active" }),
    Not(Filter { priority > 4 })
])
```

Then each `NOT(Filter)` computes its complement, and zones are unioned.

### NOT(OR) - De Morgan's Law

Transform using De Morgan's law: `NOT(A OR B)` = `NOT A AND NOT B`

```rust
// NOT(status = "active" OR status = "pending") becomes:
And([
    Not(Filter { status = "active" }),
    Not(Filter { status = "pending" })
])
```

Then each `NOT(Filter)` computes its complement, and zones are intersected.

### NOT(NOT X) - Double Negation

Double negation is eliminated: `NOT NOT X` = `X`

```rust
// NOT NOT status = "active" becomes:
Filter { status = "active" }
```

## Zone Combination Logic

`ZoneGroupCollector` recursively traverses the FilterGroup tree:

### AND Combination

Intersect zones from all children:

```rust
// AND(A, B, C): intersect zones from A, B, and C
// Early exit: if any child has no zones, return empty
```

**Example**: `AND([zone_1, zone_2], [zone_2, zone_3])` = `[zone_2]`

### OR Combination

Union zones from all children:

```rust
// OR(A, B, C): union zones from A, B, and C
```

**Example**: `OR([zone_1, zone_2], [zone_2, zone_3])` = `[zone_1, zone_2, zone_3]` (deduplicated)

### NOT Combination

Compute complement (see Smart NOT Handling above).

## Index Strategies

Each filter is assigned an index strategy based on the operation and field type:

- **ZoneXorIndex**: Equality comparisons (`=`, `IN`) on indexed fields
- **ZoneSuRF**: Range comparisons (`>`, `>=`, `<`, `<=`) on indexed fields
- **FullScan**: Fallback when no index is available

**Example**: `priority > 4` uses `ZoneSuRF` to find zones with `priority` values greater than 4.

## Performance Optimizations

### Filter Deduplication

Duplicate filters are collected only once:

```rust
// WHERE status = "active" AND status = "active"
// Only collects zones once for status = "active"
```

### Zone Cache

Zones are cached per filter key to avoid redundant lookups:

```rust
// Cache key: "status:Eq:active"
// Zones: [zone_1, zone_3]
```

### Early Exit for AND

If any child of an AND has no zones, return empty immediately:

```rust
// AND(A, B) where A has no zones → return [] immediately
// No need to collect zones for B
```

### Cross-Segment Zone Intersection

AND operations intersect zones by both `zone_id` and `segment_id`:

```rust
// Zone from segment_1, zone_2 AND zone from segment_2, zone_2
// Do NOT intersect (different segments)
```

## Examples

### Simple AND

```sneldb
QUERY orders WHERE status = "active" AND priority > 5
```

1. Build FilterGroup: `And([Filter(status="active"), Filter(priority>5)])`
2. Collect zones: `status="active"` → `[zone_1, zone_3]`, `priority>5` → `[zone_2, zone_3]`
3. Intersect: `[zone_3]`
4. Scan only `zone_3` for both filters

### IN with AND

```sneldb
QUERY orders WHERE id IN (1, 2, 3) AND status = "active"
```

1. Expand IN: `Or([Filter(id=1), Filter(id=2), Filter(id=3)])`
2. Build FilterGroup: `And([Or([...]), Filter(status="active")])`
3. Collect zones: `id=1` → `[zone_1]`, `id=2` → `[zone_2]`, `id=3` → `[zone_3]`, `status="active"` → `[zone_1, zone_3]`
4. Union IN zones: `[zone_1, zone_2, zone_3]`
5. Intersect with status: `[zone_1, zone_3]`

### Complex Parentheses

```sneldb
QUERY orders WHERE ((status = "active" OR status = "pending") AND priority > 4) OR category = "A"
```

1. Build FilterGroup:
   ```
   Or([
       And([
           Or([Filter(status="active"), Filter(status="pending")]),
           Filter(priority>4)
       ]),
       Filter(category="A")
   ])
   ```
2. Collect zones for each filter
3. Combine: Inner OR → union, then AND → intersect, then outer OR → union

### NOT Operation

```sneldb
QUERY orders WHERE NOT status = "active"
```

1. Build FilterGroup: `Not(Filter(status="active"))`
2. Get all zones: `[zone_0, zone_1, zone_2, zone_3]`
3. Get matching zones: `status="active"` → `[zone_1, zone_3]`
4. Compute complement: `[zone_0, zone_2]`

## Invariants

- **Zone uniqueness**: Zones are deduplicated by `(zone_id, segment_id)` before combination
- **Filter deduplication**: Identical filters (same column, operation, value) are collected only once
- **Early exit**: AND operations return empty immediately if any child has no zones
- **Complement correctness**: NOT operations correctly compute all zones minus matching zones
- **De Morgan's laws**: NOT(AND) and NOT(OR) are correctly transformed

## Further Reading

- [Query and Replay](./query_replay.md) - High-level query execution flow
- [Storage Engine](./storage_engine.md) - Zone structure and segment layout
- [Index Strategies](../engine/core/read/index_strategy.rs) - How filters use indexes

