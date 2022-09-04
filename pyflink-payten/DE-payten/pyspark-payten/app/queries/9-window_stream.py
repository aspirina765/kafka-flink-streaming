import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def main():
    spark = (
        SparkSession.builder.master("spark://spark-master:7077")
        .appName("9-window_stream")
        .getOrCreate()
    )

    lines = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "host.docker.internal:9092")
        .option("subscribe", "transactions")
        .option("startingOffsets", "latest")
        .load()
    )

    string_df = lines.selectExpr("CAST(value AS STRING)")
    schema = StructType(
        [
            StructField("transaction_amount", StringType()),
            StructField("transaction_date", TimestampType()),
        ]
    )
    json_df = string_df.withColumn(
        "jsonData", from_json(
            col("value"), schema)).select("jsondata.*")
    w = (
        json_df.withWatermark("transaction_date", "30 seconds")
        .groupBy(window("transaction_date", "30 seconds "))
        .agg(
            sum("transaction_amount").alias("sum_ta"),
            count("transaction_amount").alias("count_ta"),
        )
    )
    w = w.select(
        "sum_ta",
        "count_ta",
        current_timestamp(),
        w.window.end.cast("string").alias("maxtime"),
    )

    w.writeStream.outputMode("append").format("csv").option(
        "checkpointLocation", "/app/check"
    ).option("path", "/app/output/9_window_stream_output")\
        .start().awaitTermination()


if __name__ == "__main__":
    main()
