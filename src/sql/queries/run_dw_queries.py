import os
import re
import sys
from pathlib import Path

def connect_mysql():
    host = os.getenv("DW_HOST", "localhost")
    port = int(os.getenv("DW_PORT", "3306"))
    user = os.getenv("DW_USER", "root")
    password = os.getenv("DW_PASSWORD", "")
    database = os.getenv("DW_DATABASE", "")

    if not database:
        print("ERROR: Define DW_DATABASE (nome da base).")
        sys.exit(1)

    try:
        import mysql.connector  # type: ignore
        conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=database, autocommit=True, connection_timeout=10
        )
        return conn, "mysql.connector"
    except Exception:
        try:
            import pymysql  # type: ignore
            conn = pymysql.connect(
                host=host, port=port, user=user, password=password, database=database,
                cursorclass=pymysql.cursors.DictCursor
            )
            return conn, "pymysql"
        except Exception as e:
            print("ERROR: Falha a ligar ao MySQL.")
            print("Instala um driver:")
            print("  pip install mysql-connector-python")
            print("  pip install pymysql")
            print(f"Detalhes: {e}")
            sys.exit(1)

def read_query_files(folder: Path):
    files = sorted([p for p in folder.glob("*.sql") if p.is_file()])
    items = []
    for p in files:
        sql = p.read_text(encoding="utf-8", errors="ignore")
        first_line = sql.strip().splitlines()[0].strip() if sql.strip() else ""
        title = first_line.lstrip("- ").strip() if first_line.startswith("--") else p.stem
        items.append((p.name, title, sql))
    return items

def parse_params(sql_text: str):
    m = re.search(r"^\s*--\s*params\s*:\s*(.+?)\s*$", sql_text, flags=re.IGNORECASE | re.MULTILINE)
    if not m:
        return []
    raw = m.group(1)
    parts = [x.strip() for x in raw.split(",") if x.strip()]
    params = []
    for part in parts:
        if ":" in part:
            name, typ = [x.strip() for x in part.split(":", 1)]
        else:
            name, typ = part, "str"
        params.append((name, typ.lower()))
    return params

def coerce(value: str, typ: str):
    if typ in ("int", "integer", "bigint"):
        return int(value)
    if typ in ("float", "double", "decimal"):
        return float(value)
    return value

def print_rows(rows, max_rows=30):
    if rows is None:
        print("(no result set)")
        return
    if isinstance(rows, list) and len(rows) == 0:
        print("(0 rows)")
        return

    if isinstance(rows[0], dict):
        cols = list(rows[0].keys())
        data = [[str(r.get(c, "")) for c in cols] for r in rows[:max_rows]]
    else:
        cols = [f"col{i+1}" for i in range(len(rows[0]))]
        data = [[str(x) for x in r] for r in rows[:max_rows]]

    widths = [max(len(cols[i]), max((len(row[i]) for row in data), default=0)) for i in range(len(cols))]
    header = " | ".join(cols[i].ljust(widths[i]) for i in range(len(cols)))
    sep = "-+-".join("-"*w for w in widths)

    print(header)
    print(sep)
    for row in data:
        print(" | ".join(row[i].ljust(widths[i]) for i in range(len(cols))))
    if len(rows) > max_rows:
        print(f"... ({len(rows)} rows total, showing first {max_rows})")
    else:
        print(f"({len(rows)} rows)")

def run_query(conn, driver_name: str, sql_text: str, params):
    sql = sql_text.strip()
    while sql.endswith(";"):
        sql = sql[:-1].rstrip()

    if driver_name == "mysql.connector":
        conn.ping(reconnect=True, attempts=3, delay=1)

        cur = conn.cursor(dictionary=True, buffered=True)
        try:
            cur.execute(sql, params)
            return cur.fetchall() if cur.with_rows else None
        finally:
            try:
                cur.close()
            except Exception:
                pass
    else:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall() if cur.description else None


def main():
    folder = Path(__file__).resolve().parent
    conn, driver = connect_mysql()
    print(f"Connected using: {driver} (host={os.getenv('DW_HOST','localhost')}, db={os.getenv('DW_DATABASE','')})")

    while True:
        items = read_query_files(folder)
        print("\nAvailable queries:")
        for i, (fname, title, _) in enumerate(items, start=1):
            print(f"  {i:02d}) {fname}  —  {title}")

        print("\nChoose a query number to run, or type:")
        print("  r  = reload list")
        print("  q  = quit")
        choice = input("> ").strip().lower()

        if choice == "q":
            break
        if choice in ("r", ""):
            continue
        if not choice.isdigit():
            print("Please enter a number, r, or q.")
            continue

        idx = int(choice)
        if idx < 1 or idx > len(items):
            print("Invalid option.")
            continue

        fname, _, sql_text = items[idx-1]
        param_specs = parse_params(sql_text)
        params = []
        if param_specs:
            print(f"\nThis query expects parameters: {', '.join([f'{n}:{t}' for n,t in param_specs])}")
            for name, typ in param_specs:
                val = input(f"  {name} ({typ}) = ").strip()
                params.append(coerce(val, typ))

        print(f"\n--- Running: {fname} ---")
        try:
            rows = run_query(conn, driver, sql_text, tuple(params))
            print_rows(rows, max_rows=10000)
        except Exception as e:
            msg = str(e)
            if driver == "mysql.connector" and "MySQL Connection not available" in msg:
                try:
                    conn.close()
                except Exception:
                    pass
                conn, driver = connect_mysql()
                try:
                    rows = run_query(conn, driver, sql_text, tuple(params))
                    print_rows(rows, max_rows=10000)
                    continue
                except Exception as e2:
                    print(f"ERROR running query (after reconnect): {e2}")
            else:
                print(f"ERROR running query: {e}")

    try:
        conn.close()
    except Exception:
        pass
    print("Bye.")

if __name__ == "__main__":
    main()
