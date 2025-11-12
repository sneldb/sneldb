use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::Path;

use crate::engine::core::FieldXorFilter;
use crate::engine::core::ZoneIndex;
use crate::engine::core::ZoneMeta;
use crate::engine::core::column::compression::compressed_column_index::CompressedColumnIndex;
use crate::engine::core::column::compression::compression_codec::{
    ALGO_LZ4, CompressionCodec, FLAG_COMPRESSED, Lz4Codec,
};
use crate::engine::core::zone::enum_bitmap_index::EnumBitmapIndex;
use crate::engine::schema::registry::{MiniSchema, SchemaRecord, SchemaRegistry};
use crate::shared::storage_header::{BinaryHeader, FileKind};
use serde::Serialize;

pub fn main() {
    if std::env::var("DEBUG").map(|v| v == "true").unwrap_or(false) {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .with_env_filter("debug")
            .init();
    }

    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        eprintln!("Usage:");
        eprintln!("  convertor zone <path/to/zones.bin>");
        eprintln!("  convertor col <path/to/segment_dir> <uid> <field>");
        eprintln!("  convertor offset <path/to/segment_dir> <uid> <field>");
        eprintln!("  convertor schemas <path/to/schema_dir>");
        eprintln!("  convertor shards <path/to/cols_dir>");
        eprintln!("  convertor xorfilter <path/to/segment_dir> <uid> <field>");
        eprintln!("  convertor ebm <path/to/segment_dir> <uid> <field>");
        eprintln!("  convertor schema_records <path/to/schemas.bin>");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "index" => {
            let composite_path = Path::new(&args[2]);
            match ZoneIndex::load_from_path(composite_path) {
                Ok(composite) => match serde_json::to_string_pretty(&composite) {
                    Ok(json) => println!("{}", json),
                    Err(e) => {
                        eprintln!("Failed to serialize composite index: {}", e);
                        std::process::exit(1);
                    }
                },
                Err(e) => {
                    eprintln!(
                        "Failed to load composite index from {}: {}",
                        composite_path.display(),
                        e
                    );
                    std::process::exit(1);
                }
            }
        }
        "zone" => {
            let subzone_file_path = Path::new(&args[2]);
            match ZoneMeta::load(subzone_file_path) {
                Ok(subzones) => {
                    #[derive(Serialize)]
                    struct Wrapper<'a> {
                        subzones: &'a [ZoneMeta],
                    }
                    let wrapper = Wrapper {
                        subzones: &subzones,
                    };
                    match serde_json::to_string_pretty(&wrapper) {
                        Ok(json) => println!("{}", json),
                        Err(e) => {
                            eprintln!("Failed to serialize subzones: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!(
                        "Failed to load subzones from {}: {}",
                        subzone_file_path.display(),
                        e
                    );
                    std::process::exit(1);
                }
            }
        }

        "col" => {
            if args.len() != 5 {
                eprintln!("Usage: convertor col <path/to/segment_dir> <uid> <field>");
                std::process::exit(1);
            }

            let segment_path = Path::new(&args[2]);
            let uid = &args[3];
            let field = &args[4];

            // Load the compression index (.zfc)
            let zfc_path = segment_path.join(format!("{}_{}.zfc", uid, field));
            let index = match CompressedColumnIndex::load_from_path(&zfc_path) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!(
                        "Failed to load .zfc index from {}: {}",
                        zfc_path.display(),
                        e
                    );
                    std::process::exit(1);
                }
            };

            // Open the column file
            let col_path = segment_path.join(format!("{}_{}.col", uid, field));
            let file = match File::open(&col_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to open column file: {}", e);
                    std::process::exit(1);
                }
            };
            let mut reader = BufReader::new(file);

            // Validate header
            match BinaryHeader::read_from(&mut reader) {
                Ok(h) => {
                    if h.magic != FileKind::SegmentColumn.magic() {
                        eprintln!("Invalid column magic");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read column header: {}", e);
                    std::process::exit(1);
                }
            }

            let codec = Lz4Codec;
            let mut combined: HashMap<u32, Vec<String>> = HashMap::new();

            // Process each zone in sorted order
            let mut zone_ids: Vec<_> = index.entries.keys().copied().collect();
            zone_ids.sort();

            for zone_id in zone_ids {
                let entry = index.entries.get(&zone_id).unwrap();

                // Seek to the compressed block
                if let Err(e) = reader.seek(SeekFrom::Start(entry.block_start)) {
                    eprintln!("Failed to seek to zone {}: {}", zone_id, e);
                    std::process::exit(1);
                }

                // Read the compressed block
                let mut compressed_bytes = vec![0u8; entry.comp_len as usize];
                if let Err(e) = reader.read_exact(&mut compressed_bytes) {
                    eprintln!("Failed to read compressed data for zone {}: {}", zone_id, e);
                    std::process::exit(1);
                }

                // Decompress
                let uncompressed =
                    match codec.decompress(&compressed_bytes, entry.uncomp_len as usize) {
                        Ok(data) => data,
                        Err(e) => {
                            eprintln!("Failed to decompress zone {}: {}", zone_id, e);
                            std::process::exit(1);
                        }
                    };

                // Parse uncompressed data: [u16 len][bytes] repeated
                let mut cursor = 0;
                let mut values = Vec::new();
                while cursor < uncompressed.len() {
                    if cursor + 2 > uncompressed.len() {
                        break;
                    }
                    let len = u16::from_le_bytes([uncompressed[cursor], uncompressed[cursor + 1]])
                        as usize;
                    cursor += 2;

                    if cursor + len > uncompressed.len() {
                        eprintln!(
                            "Invalid length {} at cursor {} for zone {}",
                            len,
                            cursor - 2,
                            zone_id
                        );
                        break;
                    }

                    let value_bytes = &uncompressed[cursor..cursor + len];
                    cursor += len;

                    match String::from_utf8(value_bytes.to_vec()) {
                        Ok(value) => values.push(value),
                        Err(e) => {
                            eprintln!(
                                "UTF-8 error in zone {} at position {}: {}",
                                zone_id,
                                cursor - len,
                                e
                            );
                            values.push(format!("<invalid UTF-8: {} bytes>", len));
                        }
                    }
                }

                combined.insert(zone_id, values);
            }

            #[derive(Serialize)]
            struct Wrapper {
                col_values: std::collections::HashMap<u32, Vec<String>>,
            }

            let wrapper = Wrapper {
                col_values: combined,
            };
            match serde_json::to_string_pretty(&wrapper) {
                Ok(json) => println!("{}", json),
                Err(e) => {
                    eprintln!("Failed to serialize col values: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "offset" => {
            if args.len() != 5 {
                eprintln!("Usage: convertor offset <path/to/segment_dir> <uid> <field>");
                std::process::exit(1);
            }
            let segment_path = Path::new(&args[2]);
            let uid = &args[3];
            let field = &args[4];
            let offset_path = segment_path.join(format!("{}_{}.zfc", uid, field));
            let file = match File::open(&offset_path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Failed to open .zfc file: {}", e);
                    std::process::exit(1);
                }
            };
            let mut reader = BufReader::new(file);
            // Validate header
            match BinaryHeader::read_from(&mut reader) {
                Ok(h) => {
                    if h.magic != FileKind::ZoneOffsets.magic() {
                        eprintln!("Invalid .zfc magic");
                        std::process::exit(1);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read .zfc header: {}", e);
                    std::process::exit(1);
                }
            }

            // Parse structured offsets: [u32 zone_id][u32 count][u64 * count]...
            let mut result: std::collections::BTreeMap<u32, Vec<u64>> =
                std::collections::BTreeMap::new();
            loop {
                let mut zid_buf = [0u8; 4];
                let mut cnt_buf = [0u8; 4];
                if let Err(_) = reader.read_exact(&mut zid_buf) {
                    break;
                }
                if let Err(_) = reader.read_exact(&mut cnt_buf) {
                    break;
                }
                let zone_id = u32::from_le_bytes(zid_buf);
                let count = u32::from_le_bytes(cnt_buf) as usize;
                let mut vec = Vec::with_capacity(count);
                for _ in 0..count {
                    let mut obuf = [0u8; 8];
                    if let Err(_) = reader.read_exact(&mut obuf) {
                        break;
                    }
                    vec.push(u64::from_le_bytes(obuf));
                }
                result.insert(zone_id, vec);
            }
            match serde_json::to_string_pretty(&result) {
                Ok(json) => println!("{}", json),
                Err(e) => {
                    eprintln!("Failed to serialize offsets: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "schemas" => {
            if args.len() != 3 {
                eprintln!("Usage: convertor schemas <path/to/schema_dir>");
                std::process::exit(1);
            }
            let schema_dir = Path::new(&args[2]);
            let schema_path = schema_dir.join("schemas.bin");
            match SchemaRegistry::new_with_path(schema_path) {
                Ok(registry) => {
                    let all = registry.get_all();
                    match serde_json::to_string_pretty(&all) {
                        Ok(json) => println!("{}", json),
                        Err(e) => {
                            eprintln!("Failed to serialize schemas: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to load schemas: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "schema" => {
            if args.len() != 4 {
                eprintln!("Usage: convertor schema <path/to/schema_dir> <event_type>");
                std::process::exit(1);
            }
            let schema_dir = Path::new(&args[2]);
            let event_type = &args[3];
            let schema_path = schema_dir.join("schemas.bin");

            match SchemaRegistry::new_with_path(schema_path) {
                Ok(registry) => {
                    #[derive(Serialize)]
                    struct SchemaInfo {
                        event_type: String,
                        schema: Option<MiniSchema>,
                        uid: Option<String>,
                    }

                    let info = SchemaInfo {
                        event_type: event_type.to_string(),
                        schema: registry.get(event_type).cloned(),
                        uid: registry.get_uid(event_type),
                    };

                    match serde_json::to_string_pretty(&info) {
                        Ok(json) => println!("{}", json),
                        Err(e) => {
                            eprintln!("Failed to serialize schema info: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to load schemas: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "schema_records" => {
            if args.len() != 3 {
                eprintln!("Usage: convertor schema_records <path/to/schemas.bin>");
                std::process::exit(1);
            }
            let schema_path = Path::new(&args[2]);

            match SchemaRegistry::new_with_path(schema_path.to_path_buf()) {
                Ok(registry) => {
                    let mut records = Vec::new();
                    for (event_type, schema) in registry.get_all() {
                        records.push(SchemaRecord {
                            uid: registry.get_uid(event_type).unwrap_or_default(),
                            event_type: event_type.clone(),
                            schema: schema.clone(),
                        });
                    }

                    match serde_json::to_string_pretty(&records) {
                        Ok(json) => println!("{}", json),
                        Err(e) => {
                            eprintln!("Failed to serialize schema records: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to load schemas: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "shards" => {
            if args.len() != 3 {
                eprintln!("Usage: convertor shards <path/to/cols_dir>");
                std::process::exit(1);
            }
            let cols_dir = Path::new(&args[2]);
            let mut result = std::collections::BTreeMap::new();
            match std::fs::read_dir(cols_dir) {
                Ok(shard_entries) => {
                    for shard_entry in shard_entries.flatten() {
                        let shard_name = shard_entry.file_name().to_string_lossy().to_string();
                        if !shard_name.starts_with("shard-") {
                            continue;
                        }
                        let shard_path = shard_entry.path();
                        let mut segments = Vec::new();
                        if let Ok(segment_entries) = std::fs::read_dir(&shard_path) {
                            for segment_entry in segment_entries.flatten() {
                                let segment_name =
                                    segment_entry.file_name().to_string_lossy().to_string();
                                if segment_name.chars().all(|c| c.is_ascii_digit()) {
                                    segments.push(segment_name);
                                }
                            }
                        }
                        segments.sort();
                        result.insert(shard_name, segments);
                    }
                    match serde_json::to_string_pretty(&result) {
                        Ok(json) => println!("{}", json),
                        Err(e) => {
                            eprintln!("Failed to serialize shard/segment list: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Failed to read cols dir: {}", e);
                    std::process::exit(1);
                }
            }
        }

        "xorfilter" => {
            if args.len() != 5 {
                eprintln!("Usage: convertor xorfilter <path/to/segment_dir> <uid> <field>");
                std::process::exit(1);
            }
            let segment_path = Path::new(&args[2]);
            let uid = &args[3];

            // Find all .xf files for this UID
            let mut filter_files = Vec::new();
            if let Ok(entries) = std::fs::read_dir(segment_path) {
                for entry in entries.flatten() {
                    let file_name = entry.file_name().to_string_lossy().to_string();
                    if file_name.starts_with(uid) && file_name.ends_with(".xf") {
                        filter_files.push(entry.path());
                    }
                }
            }

            if filter_files.is_empty() {
                eprintln!("No XOR filter files found for UID: {}", uid);
                std::process::exit(1);
            }

            // Sort files for consistent output
            filter_files.sort();

            // Test each filter
            for filter_path in filter_files {
                let field_name = filter_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.split('_').nth(1))
                    .unwrap_or("unknown");

                // Map field names to their actual names
                let actual_field_name = match field_name {
                    "context" => "context_id",
                    "event" => "event_type",
                    _ => field_name,
                };

                println!(
                    "\nTesting XOR filter for field: {} (actual: {})",
                    field_name, actual_field_name
                );
                println!("----------------------------------------");

                // Load the XOR filter
                let filter = match FieldXorFilter::load(&filter_path) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("Failed to load XOR filter: {}", e);
                        continue;
                    }
                };

                // Load the column file
                let col_path = segment_path.join(format!("{}_{}.col", uid, actual_field_name));
                let file = match File::open(&col_path) {
                    Ok(f) => f,
                    Err(e) => {
                        eprintln!("Failed to open column file: {}", e);
                        continue;
                    }
                };
                let mut reader = BufReader::new(file);
                if let Err(e) = BinaryHeader::read_from(&mut reader) {
                    eprintln!("Failed to read column header: {}", e);
                    continue;
                }

                // Load zone metadata
                let zones_path = segment_path.join(format!("{}.zones", uid));
                let mut zone_metas = match ZoneMeta::load(&zones_path) {
                    Ok(z) => z,
                    Err(e) => {
                        eprintln!("Failed to load zones: {}", e);
                        continue;
                    }
                };
                let zone_metas = ZoneMeta::sort_by(&mut zone_metas, "zone_id");

                let mut total_values = 0;
                let mut false_negatives = 0;
                let mut sample_values = Vec::new();

                // Read values from each zone
                for zone_meta in zone_metas.iter() {
                    for _ in zone_meta.start_row..=zone_meta.end_row {
                        let mut len_buf = [0u8; 2];
                        if reader.read_exact(&mut len_buf).is_err() {
                            break;
                        }
                        let value_len = u16::from_le_bytes(len_buf) as usize;

                        let mut value_bytes = vec![0u8; value_len];
                        if reader.read_exact(&mut value_bytes).is_err() {
                            break;
                        }
                        let value = String::from_utf8(value_bytes).unwrap();

                        let contains = filter.contains(&value);
                        if !contains {
                            false_negatives += 1;
                            if sample_values.len() < 5 {
                                sample_values.push(value.clone());
                            }
                        }
                        total_values += 1;
                    }
                }

                println!("Filter Statistics:");
                println!("  Total values tested: {}", total_values);
                println!("  False negatives: {}", false_negatives);
                println!(
                    "  False negative rate: {:.2}%",
                    (false_negatives as f64 / total_values as f64) * 100.0
                );

                if !sample_values.is_empty() {
                    println!("\nSample false negatives:");
                    for value in sample_values {
                        println!("  {}", value);
                    }
                }

                #[derive(Serialize)]
                struct FilterInfo {
                    path: String,
                    size_bytes: usize,
                }
                let info = FilterInfo {
                    path: filter_path.to_string_lossy().to_string(),
                    size_bytes: std::fs::metadata(&filter_path)
                        .map(|m| m.len() as usize)
                        .unwrap_or(0),
                };
                match serde_json::to_string_pretty(&info) {
                    Ok(json) => println!("\nFilter Info:\n{}", json),
                    Err(e) => {
                        eprintln!("Failed to serialize filter info: {}", e);
                        continue;
                    }
                }
            }
        }

        "ebm" => {
            if args.len() != 5 {
                eprintln!("Usage: convertor ebm <path/to/segment_dir> <uid> <field>");
                std::process::exit(1);
            }

            use serde::Serialize;

            let segment_path = Path::new(&args[2]);
            let uid = &args[3];
            let field = &args[4];
            let ebm_path = segment_path.join(format!("{}_{}.ebm", uid, field));

            let index = match EnumBitmapIndex::load(&ebm_path) {
                Ok(i) => i,
                Err(e) => {
                    eprintln!("Failed to load EBM from {}: {}", ebm_path.display(), e);
                    std::process::exit(1);
                }
            };

            // Decode bitmaps to row positions for readability
            fn decode_positions(bytes: &[u8], rows_per_zone: u16) -> Vec<usize> {
                let mut rows = Vec::new();
                let max = rows_per_zone as usize;
                for i in 0..max {
                    let byte = i / 8;
                    let bit = i % 8;
                    if byte < bytes.len() {
                        if (bytes[byte] & (1u8 << bit)) != 0 {
                            rows.push(i);
                        }
                    }
                }
                rows
            }

            use std::collections::BTreeMap;

            #[derive(Serialize)]
            struct EbmDump {
                path: String,
                variants: Vec<String>,
                rows_per_zone: u16,
                zones: BTreeMap<u32, BTreeMap<String, Vec<usize>>>,
            }

            let mut zones: BTreeMap<u32, BTreeMap<String, Vec<usize>>> = BTreeMap::new();
            for (zone_id, bitsets) in &index.zone_bitmaps {
                let mut per_variant: BTreeMap<String, Vec<usize>> = BTreeMap::new();
                for (vid, bits) in bitsets.iter().enumerate() {
                    if let Some(name) = index.variants.get(vid) {
                        per_variant
                            .insert(name.clone(), decode_positions(bits, index.rows_per_zone));
                    }
                }
                zones.insert(*zone_id, per_variant);
            }

            let dump = EbmDump {
                path: ebm_path.to_string_lossy().to_string(),
                variants: index.variants.clone(),
                rows_per_zone: index.rows_per_zone,
                zones,
            };

            match serde_json::to_string_pretty(&dump) {
                Ok(json) => println!("{}", json),
                Err(e) => {
                    eprintln!("Failed to serialize EBM dump: {}", e);
                    std::process::exit(1);
                }
            }
        }

        _ => {
            eprintln!("Unknown mode: {}", args[1]);
            eprintln!(
                "Expected 'zone', 'col', 'offset', 'schemas', 'shards', 'xorfilter', 'schema_records'"
            );
            std::process::exit(1);
        }
    }
}
