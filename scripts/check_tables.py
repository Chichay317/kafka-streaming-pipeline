import sys
import pathlib

project_root = pathlib.Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.database import get_engine
from sqlalchemy import text

engine = get_engine()
with engine.connect() as conn:
    res = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"))
    tables = [row[0] for row in res.fetchall()]
    print("Public tables:", tables)

    for t in ("events_per_second", "sensor_rolling_avg"):
        r = conn.execute(text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{t}';"))
        print(f"\nColumns for {t}:")
        for col in r.fetchall():
            print(" ", col)
