import argparse
import getpass
import sys
import time
import socket
import csv
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory
import sqlalchemy
import pymysql
import adbc_driver_flightsql.dbapi as flight_sql
from tabulate import tabulate
import threading
import itertools
import psutil
import os

def get_flight_connection(host, port, user, password, database=None):
    uri = f"grpc://{host}:{port}"
    db_kwargs = {
        "username": user,
        "password": password
    }
    # For Arrow Flight SQL, the database is often passed as 'catalog'
    if database:
        db_kwargs["adbc.flight.sql.rpc.head.catalog"] = database
    return flight_sql.connect(uri=uri, db_kwargs=db_kwargs)

def get_alchemy_engine(host, port, user, password, database=None):
    db_path = f"/{database}" if database else "/"
    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}{db_path}"
    return sqlalchemy.create_engine(conn_str, isolation_level="AUTOCOMMIT")

def get_mysql_connection(host, port, user, password, database=None):
    return pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database,
        autocommit=True
    )

def _proxy_connect(proxy_host, proxy_port, real_connect, address, timeout=None, source_address=None):
    proxy_sock = real_connect((proxy_host, proxy_port), timeout, source_address)
    connect_cmd = f'CONNECT {address[0]}:{address[1]} HTTP/1.1\r\nHost: {address[0]}\r\n\r\n'.encode()
    proxy_sock.sendall(connect_cmd)
    response = proxy_sock.recv(4096)
    if not (response.startswith(b'HTTP/1.1 200') or response.startswith(b'HTTP/1.0 200')):
        proxy_sock.close()
        raise OSError(f'Proxy CONNECT failed: {response!r}')
    return proxy_sock

def setup_proxy(proxy_str):
    if not proxy_str:
        return

    try:
        if ':' not in proxy_str:
            raise ValueError
        proxy_host, proxy_port = proxy_str.rsplit(':', 1)
        proxy_port = int(proxy_port)
    except ValueError:
        print(f"Error: Invalid proxy format '{proxy_str}'. Expected 'host:port'.")
        sys.exit(1)

    real_create_connection = socket.create_connection

    def proxy_create_connection(address, timeout=None, source_address=None):
        return _proxy_connect(proxy_host, proxy_port, real_create_connection, address, timeout, source_address)

    socket.create_connection = proxy_create_connection

class Spinner:
    def __init__(self, message="Executing query... "):
        self.spinner = itertools.cycle(['-', '\\', '|', '/'])
        self.stop_running = threading.Event()
        self.message = message
        self.start_time = time.time()
        self.process = psutil.Process(os.getpid())
        self.total_cpu = 0.0
        self.total_memory_mb = 0.0
        self.sample_count = 0
        self.thread = threading.Thread(target=self._spin, daemon=True)

    def _spin(self):
        # Initial call to cpu_percent to initialize
        self.process.cpu_percent()
        while not self.stop_running.is_set():
            cpu = self.process.cpu_percent()
            memory_mb = self.process.memory_info().rss / (1024 * 1024)
            self.total_cpu += cpu
            self.total_memory_mb += memory_mb
            self.sample_count += 1
            elapsed = time.time() - self.start_time
            mins, secs = divmod(int(elapsed), 60)
            status = f" [{mins:02d}:{secs:02d}] [CPU: {cpu:.1f}% | MEM: {memory_mb:.1f} MB] "
            sys.stdout.write(f"\r{self.message}{next(self.spinner)}{status}")
            sys.stdout.flush()
            time.sleep(0.1)
        # Clear the spinner line
        sys.stdout.write("\r" + " " * (len(self.message) + 80) + "\r")
        sys.stdout.flush()

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop_running.set()
        self.thread.join()

def main():
    parser = argparse.ArgumentParser(description="StarRocks Python CLI Client", add_help=False)
    parser.add_argument("-P", "--port", type=str, help="Port")
    parser.add_argument("-h", "--host", type=str, help="Host")
    parser.add_argument("-u", "--user", type=str, help="User")
    parser.add_argument("-p", "--password", type=str, nargs='?', const=True, help="Password (leave empty to prompt)")
    parser.add_argument("-d", "--database", type=str, help="Initial database")
    parser.add_argument("-x", "--proxy", type=str, help="HTTP Proxy (host:port)")
    parser.add_argument("-m", "--mode", type=int, choices=[1, 2, 3, 4], help="Mode: 1 (AlchemySQL), 2 (Arrow Flight SQL), 3 (MySQL Direct) or 4 (AlchemySQL stream mode)")
    parser.add_argument("-mrb", "--max-row-buffer", type=int, help="Max number of rows to buffer in memory when streaming results from DB(only for mode 4)")
    parser.add_argument("--prompt", type=str, default="StarRocks> ", help="Interactive prompt string")
    parser.add_argument("--help", action="help", help="Show this help message and exit")

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

    if args.proxy:
        setup_proxy(args.proxy)

    if not args.port or not args.host or not args.user or not args.mode:
        print("Error: Required arguments missing (-P, -h, -u, -m).")
        parser.print_help(sys.stderr)
        sys.exit(1)

    password = args.password
    # If -p is present but empty, it's evaluated as True
    # If absent, args.password is None (which we don't handle automatically here, so we must rely on truthiness or None)
    # The requirement is: "Si no se le pasa el password en la invocación, lo pedirá al usuario."
    # If argument -p is NOT passed, args.password is None.
    # If -p is passed with no value, it is True.
    if password is None or password is True:
        password = getpass.getpass(f"Password for {args.user}: ")

    engine = None
    flight_conn = None
    mysql_conn = None

    try:
        if args.mode == 1:
            print(f"Connecting using AlchemySQL (PyMySQL) to {args.host}:{args.port}...")
            engine = get_alchemy_engine(args.host, args.port, args.user, password, args.database)
            # Test connection
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
        elif args.mode == 2:
            print(f"Connecting using Arrow Flight SQL to {args.host}:{args.port}...")
            flight_conn = get_flight_connection(args.host, args.port, args.user, password, args.database)
            # Test connection
            with flight_conn.cursor() as cursor:
                pass
        elif args.mode == 3:
            print(f"Connecting using MySQL (PyMySQL) to {args.host}:{args.port}...")
            mysql_conn = get_mysql_connection(args.host, args.port, args.user, password, args.database)
            # Test connection
            with mysql_conn.cursor() as cursor:
                cursor.execute("SELECT 1")
        elif args.mode == 4:
            print(f"Connecting using AlchemySQL (PyMySQL) to {args.host}:{args.port}...")
            print(f"Results will be writed to CSV instead of printed in console.")
            engine = get_alchemy_engine(args.host, args.port, args.user, password, args.database)
            # Test connection
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
    except Exception as e:
        print(f"Failed to connect to StarRocks: {e}")
        sys.exit(1)

    print("Welcome to the StarRocks Python REPL monitor.")
    print("Commands end with ;")

    session = PromptSession(history=InMemoryHistory())
    buffer = []

    while True:
        try:
            prompt_str = args.prompt if not buffer else "    -> "
            line = session.prompt(prompt_str).strip()

            if not line and not buffer:
                continue

            buffer.append(line)
            sql = " ".join(buffer)

            if sql.lower() in ("exit", "exit;", "quit", "quit;"):
                break

            if sql.endswith(";"):
                buffer = [] # Reset buffer for next query

                start_time = time.perf_counter()
                rows, columns = [], []

                avg_cpu = 0.0
                avg_memory_mb = 0.0
                max_row_buffer = int(args.max_row_buffer) if args.max_row_buffer else 1000
                try:
                    with Spinner() as spinner:
                        if args.mode == 1:
                            with engine.connect() as conn:
                                result = conn.execute(sqlalchemy.text(sql))
                                if result.returns_rows:
                                    rows = result.fetchall()
                                    columns = result.keys()
                        elif args.mode == 2:
                            with flight_conn.cursor() as cursor:
                                cursor.execute(sql)
                                if cursor.description:
                                    columns = [desc[0] for desc in cursor.description]
                                    rows = cursor.fetchall()
                        elif args.mode == 3:
                            with mysql_conn.cursor() as cursor:
                                cursor.execute(sql)
                                if cursor.description:
                                    columns = [desc[0] for desc in cursor.description]
                                    rows = cursor.fetchall()
                        elif args.mode == 4:
                            with open('query_stream_results.csv', 'w', newline='') as csvfile:
                                csv_writer = csv.writer(
                                    csvfile, delimiter=';', lineterminator='\n', escapechar='\\', quoting=csv.QUOTE_NONE
                                )
                                with engine.connect() as conn:
                                    with conn.execution_options(stream_results=True, max_row_buffer=max_row_buffer).execute(
                                        sqlalchemy.text(sql)
                                    ).mappings() as result:
                                        exec_duration = time.perf_counter() - start_time
                                        for row in result:
                                            csv_writer.writerow(list(row.values()) + [''])
                            writing_duration = time.perf_counter() - start_time - exec_duration
                            print(f"\nCSV writing ended (exec query {exec_duration:.3f} sec, format {writing_duration:.3f} sec) [Avg CPU: {avg_cpu:.1f}% | Avg MEM: {avg_memory_mb:.1f} MB]\n")
                            print(f'Results have been written to `query_stream_results.csv` in the root directory with size {os.stat("query_stream_results.csv").st_size / 1000} KB.\n')

                    n = spinner.sample_count or 1
                    avg_cpu = spinner.total_cpu / n
                    avg_memory_mb = spinner.total_memory_mb / n
                except Exception as e:
                    print(f"Error executing statement: {e}")
                    continue

                exec_duration = time.perf_counter() - start_time
                start_format_time = time.perf_counter()

                if columns and args.mode != 4:
                    with Spinner("Formatting table... "):
                        table_output = tabulate(rows, headers=columns, tablefmt="psql")
                    print(table_output)
                format_duration = time.perf_counter() - start_format_time
                print(f"{len(rows)} rows in set (exec query {exec_duration:.3f} sec, format {format_duration:.3f} sec) [Avg CPU: {avg_cpu:.1f}% | Avg MEM: {avg_memory_mb:.1f} MB]\n")

        except KeyboardInterrupt:
            buffer = []
            print("^C")
            continue
        except EOFError:
            break

    if engine:
        engine.dispose()
    if flight_conn:
        flight_conn.close()
    if mysql_conn:
        mysql_conn.close()
    print("Bye")

if __name__ == "__main__":
    main()
