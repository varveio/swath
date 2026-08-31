# Changelog

## Unreleased

### Changed

- Parquet output now exposes `key` as a STRING logical column while retaining its byte-identical
  BINARY storage. Downstream DuckDB, Spark, pandas, and similar queries therefore see the column
  type change from BLOB to VARCHAR and may need to update blob comparisons, casts, or functions.
- Parquet output now rejects malformed UTF-8 key bytes with a typed output error. Legacy captures
  containing non-UTF-8 keys remain readable, but they can no longer be re-published as Parquet or
  used as input for `--sort` output.
