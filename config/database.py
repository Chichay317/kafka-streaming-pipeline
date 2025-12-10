from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

def get_database_url():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    db = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def get_engine():
    engine = create_engine(get_database_url(), future=True)
    return engine

if __name__ == "__main__":
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))
        print("Connection test result:", result.scalar())
