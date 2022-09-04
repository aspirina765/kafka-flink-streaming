from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("2_word_count_percent")
    .getOrCreate()
)

products_wc = (
    spark.read.option("escape", '"')
    .option("multiline", True)
    .csv("/app/input/products_with_pipe.csv", header=True, sep="|")
)
products_wc.createOrReplaceTempView("products_wc")

products_wc.withColumn(
    "count", f.size(f.split(f.col("reviewText"), "like|love")) - 1
).write.csv("/app/output/2_word_count_lovelike_output", mode="overwrite")
