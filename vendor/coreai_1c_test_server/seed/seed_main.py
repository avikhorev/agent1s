"""Main entry point for database seeding."""
import os
import time
from sqlalchemy import create_engine, MetaData, text

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://odata:odata_secret@localhost:5432/odata_1c")


def main():
    print("Connecting to database...")
    engine = create_engine(DATABASE_URL, echo=False)

    retries = 10
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            break
        except Exception as e:
            print(f"  DB not ready ({e}), retrying in 3s... ({i+1}/{retries})")
            time.sleep(3)
    else:
        raise RuntimeError("Could not connect to database")

    meta = MetaData()
    with engine.connect() as conn:
        from generators.trade import seed_trade
        print("\n=== Seeding Trade Management (UT) ===")
        seed_trade(conn, meta)

        from generators.accounting import seed_accounting
        print("\n=== Seeding Accounting (BP) ===")
        seed_accounting(conn, meta)

    print("\n=== Seeding complete! ===")


if __name__ == "__main__":
    main()
