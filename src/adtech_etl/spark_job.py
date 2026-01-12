from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as _sum, count

def main():
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


if __name__ == "__main__":
    main()
