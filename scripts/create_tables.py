import sys
import pathlib
from pathlib import Path

project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.database import get_engine

def main():
    sql_path = project_root / "sql" / "create_streaming_tables.sql"
    if not sql_path.exists():
        raise FileNotFoundError(f"DDL file not found: {sql_path}")

    ddl = sql_path.read_text()
    engine = get_engine()

    print("Creating tables in the database...")
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    print("Tables created (or already existed).")

if __name__ == "__main__":
    main()
