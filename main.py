import argparse
import getpass
import sys
import time
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory
import sqlalchemy
import adbc_driver_flightsql.dbapi as flight_sql
from tabulate import tabulate
import threading
import itertools
import psutil
import os

def get_flight_connection(host, port, user, password):
    uri = f"grpc://{host}:{port}"
    db_kwargs = {
        "username": user,
        "password": password
    }
    return flight_sql.connect(uri=uri, db_kwargs=db_kwargs)

def get_alchemy_engine(host, port, user, password):
    conn_str = f"mysql+pymysql://{user}:{password}@{host}:{port}/"
    return sqlalchemy.create_engine(conn_str, isolation_level="AUTOCOMMIT")

class Spinner:
    def __init__(self, message="Executing query... "):
        self.spinner = itertools.cycle(['-', '\\', '|', '/'])
        self.stop_running = threading.Event()
        self.message = message
        self.process = psutil.Process(os.getpid())
        self.thread = threading.Thread(target=self._spin, daemon=True)

    def _spin(self):
        # Initial call to cpu_percent to initialize
        self.process.cpu_percent()
        while not self.stop_running.is_set():
            cpu = self.process.cpu_percent()
            memory_mb = self.process.memory_info().rss / (1024 * 1024)
            status = f" [CPU: {cpu:.1f}% | MEM: {memory_mb:.1f} MB] "
            sys.stdout.write(f"\r{self.message}{next(self.spinner)}{status}")
            sys.stdout.flush()
            time.sleep(0.1)
        # Clear the spinner line
        sys.stdout.write("\r" + " " * (len(self.message) + 40) + "\r")
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
    parser.add_argument("-m", "--mode", type=int, choices=[1, 2], help="Mode: 1 (AlchemySQL) or 2 (Arrow Flight SQL)")
    parser.add_argument("--prompt", type=str, default="StarRocks> ", help="Interactive prompt string")
    parser.add_argument("--help", action="help", help="Show this help message and exit")

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)

    args = parser.parse_args()

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

    try:
        if args.mode == 1:
            print(f"Connecting using AlchemySQL (PyMySQL) to {args.host}:{args.port}...")
            engine = get_alchemy_engine(args.host, args.port, args.user, password)
            # Test connection
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text("SELECT 1"))
        elif args.mode == 2:
            print(f"Connecting using Arrow Flight SQL to {args.host}:{args.port}...")
            flight_conn = get_flight_connection(args.host, args.port, args.user, password)
            # Test connection
            with flight_conn.cursor() as cursor:
                pass
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

                try:
                    with Spinner():
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
                except Exception as e:
                    print(f"Error executing statement: {e}")
                    continue

                duration = time.perf_counter() - start_time

                if columns:
                    print(tabulate(rows, headers=columns, tablefmt="psql"))

                print(f"{len(rows)} rows in set ({duration:.3f} sec)\n")

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
    print("Bye")

if __name__ == "__main__":
    main()
