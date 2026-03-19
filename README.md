# StarRocks Python CLI Client

A lightweight command-line interface for StarRocks, designed to mimic the behavior of the MySQL shell. It supports both SQLAlchemy (MySQL protocol) and Apache Arrow Flight SQL modes.

## Features

- **Interactive REPL**: Multi-line SQL command buffering until a semicolon `;` is encountered.
- **Multiple Connection Modes**: 
  - **Mode 1 (AlchemySQL)**: Standard connection via `sqlalchemy` and `pymysql`.
  - **Mode 2 (Arrow Flight SQL)**: High-performance data transfer using the `adbc-driver-flightsql` driver.
  - **Mode 3 (MySQL Direct)**: Direct connection using `pymysql` without the SQLAlchemy overhead.
  - **Mode 4 (AlchemySQL Streaming to CSV)**: Optimized streaming mode that saves query results directly to a CSV file (`query_stream_results.csv`), bypassing console output for large datasets.
- **Secure Password Handling**: Prompts for password securely if not provided via command line.
- **Formatted Results**: Automatically displays query results in a clean ASCII table.
- **Performance Metrics**: Shows row counts and precise execution time for every query.
- **Proxy Support**: Connect to StarRocks through an HTTP proxy using the `-x` parameter.

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
| `-d` | `--database` | (Optional) Initial database to connect to. |
| `-x` | `--proxy` | HTTP Proxy address and port (e.g., `172.18.24.129:6666`) |
| `-m` | `--mode` | Operating mode: `1` for AlchemySQL, `2` for Arrow Flight SQL, `3` for MySQL Direct, `4` for AlchemySQL Streaming to CSV |
| `-mrb`| `--max-row-buffer` | (Optional) Max number of rows to buffer in memory (only for Mode 4). Default: `1000` |
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

**Connect using MySQL Direct (Mode 3):**
```bash
python3 main.py -h 127.0.0.1 -P 9030 -u root -m 3
```

**Stream results to CSV (Mode 4):**
```bash
python3 main.py -h 127.0.0.1 -P 9030 -u root -m 4 -mrb 1000
```

**Connect through an HTTP Proxy:**
```bash
python3 main.py -h odin.service-insights.com -P 9843 -u pods-reporter -m 1 -x 172.18.24.129:6666
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

### CSV Output (Mode 4)

In **Mode 4**, the results are not displayed in the console. Instead, they are streamed directly to a file named `query_stream_results.csv` in the current directory. The CSV is formatted with:
- **Delimiter**: `;`
- **Line Terminator**: `\n`
- **Quoting**: None (escaped by `\`)

This mode is specifically designed for exporting large datasets efficiently while keeping a low memory footprint.

## Performance Metrics
The shell also shows the % of CPU and RAM consumed during the execution of the query. There is also a nice spinner animation while the query is executing so you know that the query is running and not frozen.

```sql
StarRocks> SELECT * FROM very.big_table;
Executing query... / [CPU: 0.2% | MEM: 6.4 MB]
```   

## License
MIT (or your specific license)
