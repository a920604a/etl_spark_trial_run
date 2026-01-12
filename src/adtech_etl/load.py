from sqlalchemy import create_engine
import pandas as pd

DB_URL = "postgresql+psycopg2://adtech_user:adtech_pass@localhost:5432/adtech"

def load_to_postgres(
    df: pd.DataFrame,
    table_name: str,
    if_exists: str = "append",
):
    engine = create_engine(DB_URL)

    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
        method="multi",   # batch insert
        chunksize=1000
    )
