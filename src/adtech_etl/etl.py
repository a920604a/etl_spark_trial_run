import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count
import logging
import time


# -------------------------
# Logging 設定
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# -------------------------
# Config
# -------------------------
DB_URL = "postgresql+psycopg2://adtech_user:adtech_pass@localhost:5432/adtech"
VALID_EVENT_TYPES = {"impression", "click", "conversion"}



def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Read all sheets from Excel and union into one DataFrame
    """
    df = pd.read_csv(file_path)
    df["source_file"] = Path(file_path).name  # lineage
    return df



def clean_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Read a CSV file into a pandas DataFrame
    """
    df = df.copy()

    # parse datetime
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # basic validation
    df = df.dropna(subset=["event_time", "user_id", "ad_id", "campaign_id"])
    df = df[df["event_type"].isin(VALID_EVENT_TYPES)]
    df = df[df["cost"] >= 0]

    # derived columns
    df["event_date"] = df["event_time"].dt.date

    return df

def spark_process():
    spark = (
        SparkSession.builder
        .appName("adtech-spark-job")
        .getOrCreate()
    )

    df = spark.read.parquet("data/clean_events")

    agg_df = (
        df.groupBy("event_date", "campaign_id")
        .agg(
            count(when(col("event_type") == "impression", True)).alias("impressions"),
            count(when(col("event_type") == "click", True)).alias("clicks"),
            _sum("cost").alias("total_cost"),
        )
        .withColumn(
            "ctr",
            when(col("impressions") > 0, col("clicks") / col("impressions"))
            .otherwise(0.0)
        )
    )

    agg_df.write.mode("overwrite").parquet("data/campaign_daily_stats")


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


if __name__ == "__main__":
     # 1️⃣ Extract
    raw_df = extract_csv(r"D:\projects\etl_spark_trial_run\data\raw\ad_events-day_2024_01_01.csv")
    
    # 2️⃣ Transform
    clean_df = clean_events(raw_df)
    
    # Optional: write cleaned CSV or Parquet for Spark
    clean_df.to_parquet(r"D:\projects\etl_spark_trial_run\data\clean_events", index=False)
    logging.info("Cleaned data written to Parquet")

    
    # 3️⃣ Load to PostgreSQL
    load_to_postgres(clean_df, table_name="ad_events_clean", if_exists="replace")
    logging.info("Data loaded to PostgreSQL")
    
    # 4️⃣ Spark aggregation
    # spark_process()
