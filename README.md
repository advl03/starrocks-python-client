# StarRocks Python CLI Client

A lightweight command-line interface for StarRocks, designed to mimic the behavior of the MySQL shell. It supports both SQLAlchemy (MySQL protocol) and Apache Arrow Flight SQL modes.

## Features

- **Interactive REPL**: Multi-line SQL command buffering until a semicolon `;` is encountered.
- **Dual Connection Modes**: 
  - **Mode 1 (AlchemySQL)**: Standard connection via `sqlalchemy` and `pymysql`.
  - **Mode 2 (Arrow Flight SQL)**: High-performance data transfer using the `adbc-driver-flightsql` driver.
- **Secure Password Handling**: Prompts for password securely if not provided via command line.
- **Formatted Results**: Automatically displays query results in a clean ASCII table.
- **Performance Metrics**: Shows row counts and precise execution time for every query.

## Installation

1. **Clone the repository**:
   ```bash
   cd /Users/id02297/Documents/Proyectos_TID/Repositories/starrocks-python-client
   ```

2. **Install dependencies**:
   It is recommended to use a virtual environment.
   ```bash
   python3 -m pip install -r requirements.txt
   ```

## Usage

Run the client using `main.py` with the required parameters.

### Basic Syntax
```bash
python3 main.py -h <host> -P <port> -u <user> -m <mode>
```

### Parameters
| Flag | Long Flag | Description |
| --- | --- | --- |
| `-h` | `--host` | StarRocks Host address (e.g., `127.0.0.1`) |
| `-P` | `--port` | Connection port (e.g., `9030` for MySQL, `9408` for Arrow Flight) |
| `-u` | `--user` | Database username |
| `-p` | `--password` | Database password. If passed without a value, you will be prompted securely. |
| `-m` | `--mode` | Operating mode: `1` for AlchemySQL, `2` for Arrow Flight SQL |
| | `--prompt` | (Optional) Customize the prompt string (default: `StarRocks> `) |

### Examples

**Connect using AlchemySQL (Mode 1):**
```bash
python3 main.py -h 127.0.0.1 -P 9030 -u root -m 1
```

**Connect using Arrow Flight SQL (Mode 2):**
```bash
python3 main.py -h 127.0.0.1 -P 9408 -u root -m 2
```

## Interactive Shell

Once connected, you can execute SQL commands just like in a standard database shell. Remember to end your commands with a semicolon `;`.

```sql
StarRocks> SELECT count(*) FROM demo.test_table;
+----------+
| count(*) |
|----------|
| 1000000  |
+----------+
1 rows in set (0.124 sec)

StarRocks> exit;
Bye
```

## License
MIT (or your specific license)
