## Testing

### Unit tests

Run:

```bash
SNELDB_CONFIG='config.test.toml' cargo test
```

### E2E tests

Run:

```bash
SNELDB_CONFIG='config.toml' cargo run --bin test_runner
```
