# File Formats and Data Layout

## What it is

- The on-disk layout for shards and segments, and the binary formats used for columns, offsets, zone metadata, indexes, and schemas.
- These formats are append-friendly, read-optimized, and simple to parse with memory maps.

## Core pieces

- Segments — `segment-xxxxx/` directories under each shard.
- Columns — `{uid}_{field}.col` files storing values with length prefixes.
- Zone Compressed Offsets — `{uid}_{field}.zfc` files listing per-zone compressed block metadata and in-block offsets.
- Zone Metadata — `{uid}.zones` containing per-zone min/max timestamps and row ranges.
- Zone Index — `{uid}.idx` mapping context_id values to zone ids.
- XOR Filters — `{uid}_{field}.xf` per-field filters for fast membership tests.
- Enum Bitmap Indexes — `{uid}_{field}.ebm` per-enum-field bitmaps for zone pruning.
- Zone SuRF Filters — `{uid}_{field}.zsrf` per-field per-zone succinct range filters for range pruning.
- Schemas — `schema/schemas.bin` append-only records of event type schemas and UIDs.

## Binary headers

- All binary files now begin with a fixed, 20-byte header to improve safety and detect corruption.
- Header layout (little-endian):
  - 8 bytes: MAGIC (ASCII tag identifying file kind)
  - 2 bytes: VERSION (u16)
  - 2 bytes: FLAGS (u16)
  - 4 bytes: RESERVED (u32)
  - 4 bytes: HEADER_CRC32 (u32) computed over MAGIC+VERSION+FLAGS+RESERVED
- WAL logs remain newline-delimited JSON without a binary header.

Magic strings per file kind:

- Columns (`.col`): `EVDBCOL\0`
- Zone Compressed Offsets (`.zfc`): `EVDBZCF\0`
- Zone Metadata (`.zones`): `EVDBZON\0`
- Zone Index (`.idx` per-UID/context): `EVDBUID\0`
- XOR Filters (`.xf`): `EVDBXRF\0`
- Zone SuRF Filters (`.zsrf`): `EVDBZSF\0`
- Shard Segment Index (`segments.idx`): `EVDBSIX\0`
- Schemas (`schemas.bin`): `EVDBSCH\0`
- Enum Bitmap Index (`.ebm`): `EVDBEBM\0`
- Event Snapshots (`.snp`): `EVDBSNP\0`
- Snapshot Metadata (`.smt`): `EVDBSMT\0`

Compatibility and migration:

- Readers tolerate legacy files that lack headers and continue to parse them.
- New writers always prepend the header.
- A future strict mode may enforce headers on read.

## Directory layout

```
data/
├── cols/
│   ├── shard-0/
│   │   └── segment-00000/
│   │       ├── {uid}_{field}.col
│   │       ├── {uid}_{field}.zfc
│   │       ├── {uid}.zones
│   │       ├── {uid}.idx
│   │       ├── {uid}_{field}.xf
│   │       ├── {uid}_{field}.zsrf
│   │       └── {uid}_{field}.ebm
│   └── shard-1/
│       └── segment-00000/
├── logs/
│   └── sneldb.log.YYYY-MM-DD
└── schema/
    └── schemas.bin
```

Snapshots are ad-hoc utility files and can be written anywhere (not tied to the segment layout). Typical usage writes them to a caller-provided path.

## Column files: `{uid}_{field}.col`

- Format per value (binary):
  - File begins with a binary header (MAGIC `EVDBCOL\0`).
  - `[u16]` little-endian length
  - `[bytes]` UTF‑8 string of the value
- Access pattern: memory-mapped and sliced using offsets.

## Zone compressed offsets: `{uid}_{field}.zfc`

- Binary layout per zone (repeated):
  - File begins with a binary header (MAGIC `EVDBZOF\0`).
  - `[u32] zone_id`
  - `[u32] count` number of offsets
  - `[u64] * count` byte offsets into the corresponding `.col`
- Purpose: enables loading only the rows for a given zone by first reading and decompressing the zone block, then slicing values using in-block offsets.

## Zone metadata: `{uid}.zones`

- Bincode-encoded `Vec<ZoneMeta>`.
- File begins with a binary header (MAGIC `EVDBZON\0`).
- Fields:
  - `zone_id: u32`
  - `uid: String`
  - `segment_id: u64`
  - `start_row: u32`
  - `end_row: u32`
  - `timestamp_min: u64`
  - `timestamp_max: u64`

## Zone index: `{uid}.idx`

- Binary map of `event_type -> context_id -> [zone_id...]`.
- Used to quickly locate candidate zones by `context_id`.
- Written via `ZoneIndex::write_to_path` and read with `ZoneIndex::load_from_path`.
- File begins with a binary header (MAGIC `EVDBUID\0`).

## XOR filters: `{uid}_{field}.xf`

- Bincode-serialized `BinaryFuse8` filter over unique field values.
- Used for fast approximate membership checks during planning.
- File begins with a binary header (MAGIC `EVDBXRF\0`).

## Zone SuRF filters: `{uid}_{field}.zsrf`

- Bincode-serialized `ZoneSurfFilter` containing `Vec<ZoneSurfEntry>`.
- Purpose: zone-level range pruning for numeric, string, and boolean fields using a succinct trie.
- File begins with a binary header (MAGIC `EVDBZSF\0`).
- Contents:
  - `entries: Vec<ZoneSurfEntry>` where each entry is `{ zone_id: u32, trie: SurfTrie }`.
  - `SurfTrie` stores compact arrays of degrees, child offsets, labels, and terminal flags.
- Built during flush/compaction by `ZoneWriter::write_all`.
- Used by `ZoneFinder` for `Gt/Gte/Lt/Lte` operations before falling back to XOR/EBM.
- Naming mirrors `.xf`/`.ebm`: per `uid` and `field`.

## Enum bitmap index: `{uid}_{field}.ebm`

- Zone-level bitmaps per enum variant for fast Eq/Neq pruning.
- File begins with a binary header (MAGIC `EVDBEBM\0`).
- Binary layout:
  - `[u16] variant_count`
  - Repeated `variant_count` times:
    - `[u16] name_len`
    - `[bytes] variant_name (UTF‑8)`
  - `[u16] rows_per_zone`
  - Repeated per zone present in the file:
    - `[u32] zone_id`
    - `[u16] variant_count_again`
    - Repeated `variant_count_again` times:
      - `[u32] bitmap_len_bytes`
      - `[bytes] packed_bitmap` (LSB-first within a byte; bit i set ⇒ row i has this variant)
- Usage: on a filter `plan = "pro"`, prune zones where the `pro` bitmap is all zeros; similarly for `!=` by checking any non-target variant has a bit set.
- Observability: use `convertor ebm <segment_dir> <uid> <field>` to dump a JSON view of per-zone row positions per variant.

## Schemas: `schema/schemas.bin`

- Append-only file of bincode-encoded `SchemaRecord` entries:
  - `uid: String`
  - `event_type: String`
  - `schema: MiniSchema`
- Loaded at startup by `SchemaRegistry`.
- File begins with a binary header (MAGIC `EVDBSCH\0`).

## Shard segment index: `segments.idx`

- Bincode-encoded `Vec<SegmentEntry>`; file begins with a binary header (MAGIC `EVDBSIX\0`).

## Why this design

- Immutable segments + append-only metadata simplify recovery and concurrency.
- Memory-mappable, length-prefixed encodings keep parsing simple and fast.
- Separate files per concern (values, offsets, metadata, indexes) enable targeted IO.

## Operational notes

- Segment directories are named `segment-00000`, `segment-00001`, ...
- UIDs are per-event-type identifiers generated at DEFINE; filenames use `{uid}` not the event type.
- New fields simply create new `.col/.zfc/.xf` files in subsequent segments.

## Further Reading

- [Storage Engine](./storage_engine.md)
- [Sharding](./sharding.md)
- [Query & Replay](./query_replay.md)

## Event snapshots: `*.snp`

- Purpose: portable bundles of events (potentially mixed types) for export, testing, or replay.
- File begins with a binary header (MAGIC `EVDBSNP\0`).
- Binary layout after header:
  - `[u32] num_events`
  - Repeated `num_events` times:
    - `[u32] len_bytes`
    - `[bytes] JSON-serialized Event` (same schema as API/Event struct)
- Notes:
  - Events are serialized as JSON for compatibility (payloads can contain arbitrary JSON values).
  - Readers stop gracefully on truncated data (warn and return successfully with the parsed prefix).

## Snapshot metadata: `*.smt`

- Purpose: describes snapshot ranges per `(uid, context_id)` with min/max timestamps.
- File begins with a binary header (MAGIC `EVDBSMT\0`).
- Binary layout after header:
  - `[u32] num_records`
  - Repeated `num_records` times:
    - `[u32] len_bytes`
    - `[bytes] JSON-serialized SnapshotMeta { uid, context_id, from_ts, to_ts }`
- Notes:
  - JSON is used for the same reasons as snapshots (arbitrary strings/IDs, forward-compat fields).
  - Readers stop gracefully on truncated data (warn and return successfully with the parsed prefix).
