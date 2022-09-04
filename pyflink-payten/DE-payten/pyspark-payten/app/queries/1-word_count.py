from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("1_word_count")
    .getOrCreate()
)

products_wc = (
    spark.read.option("escape", '"')
    .option("multiline", True)
    .csv("/app/input/proo.csv", header=True, sep="|")
)
products_wc.createOrReplaceTempView("products_wc")

spark.sql(
    """
    SELECT reviewText, count(*) FROM products_wc GROUP BY reviewText;
"""
).write.csv("/app/output/1_word_count_output", mode="overwrite")
