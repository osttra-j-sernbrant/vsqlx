# vsqlx

`vsqlx` is a modern, lightweight, high-performance command-line client for [Vertica](https://www.vertica.com/).

Designed as a drop-in replacement for Vertica's native `vsql` CLI, `vsqlx` preserves core `vsql` command-line flags while adding modern output formats (such as Apache Parquet and JSON), automatic `.env` loading, and pure Go portability with zero CGO dependencies.

---

## Features

- **`vsql` Compatibility:** Drop-in flag compatibility (`-h`, `-p`, `-U`, `-w`, `-d`, `-m`, `-c`, `-f`, `-o`, `-P`, `-V`).
- **Multiple Output Formats:**
  - `table` (default): ASCII formatted table matching `vsql` output and row counts.
  - `csv`: RFC 4180 compliant CSV output.
  - `json`: JSON array of objects.
  - `parquet`: Compact binary Apache Parquet format with Snappy compression.
- **Credential & Environment Management:**
  - Automatic `.env` file parsing in the working directory.
  - Standard `.pgpass` / `$PGPASSFILE` password lookup.
  - Standard environment variables (`VERTICA_HOST`, `VERTICA_PORT`, `VERTICA_USER`, `VERTICA_PASSWORD`, `VERTICA_DB`, `VERTICA_TLSMODE`).
- **Zero Dependencies:** Pure Go binary with no runtime C libraries or CGO requirements. Cross-platform support for Linux, macOS, and Windows (`amd64`, `arm64`).

---

## Installation

### Via `go install`

```bash
go install github.com/osttra-j-sernbrant/vsqlx@latest
```

### Pre-built Binaries

Download pre-compiled binaries for your platform (Linux, macOS, Windows) directly from the [GitHub Releases](https://github.com/osttra-j-sernbrant/vsqlx/releases) page.

### Build from Source

```bash
git clone https://github.com/osttra-j-sernbrant/vsqlx.git
cd vsqlx
go build -ldflags="-s -w" -o vsqlx .
```

---

## Quick Start

### Basic Query

```bash
vsqlx -h localhost -p 5433 -d mydb -U dbadmin -w secret -c "SELECT * FROM my_table LIMIT 5;"
```

### Pipe from Stdin

```bash
echo "SELECT version();" | vsqlx
```

### Execute a SQL File

```bash
vsqlx -f query.sql
```

### Export to Parquet

```bash
vsqlx -c "SELECT * FROM large_table;" -format parquet -o output.parquet
```

### Export to JSON or CSV

```bash
# JSON
vsqlx -c "SELECT id, name FROM users;" -format json

# CSV
vsqlx -c "SELECT id, name FROM users;" -format csv -o users.csv
```

### Custom NULL Representation

Matching `vsql`'s `-P null=...` option:

```bash
vsqlx -c "SELECT * FROM users;" -P null="(null)"
```

---

## Command-Line Options

| Flag | Description | Default |
| :--- | :--- | :--- |
| `-h` | Database server host | `$VERTICA_HOST` or `localhost` |
| `-p` | Database server port | `$VERTICA_PORT` or `5433` |
| `-U` | Database user name | `$VERTICA_USER` or current user / `dbadmin` |
| `-w` | Database user password | `$VERTICA_PASSWORD` or `.pgpass` |
| `-d` | Database name | `$VERTICA_DB` or current user |
| `-m` | SSL / TLS mode (`verify-full`, `require`, `prefer`, `allow`, `disable`, `none`) | `$VERTICA_TLSMODE` or `none` |
| `-c` | SQL query to execute | |
| `-f` | Path to a file containing the SQL query | |
| `-o` | Output file path | `stdout` |
| `-P` | Set printing options (`-P null=STRING`) | empty string |
| `-format` | Output format: `table`, `json`, `csv`, `parquet` | `table` |
| `-timeout` | Query timeout duration (e.g. `30s`, `5m`, `1h`) | `5m0s` |
| `-v`, `-verbose` | Enable verbose driver warnings and error logs | `false` |
| `-V`, `--version` | Output version information, then exit | |

---

## License

MIT License
