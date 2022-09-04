from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("6-join")
    .getOrCreate()
)

products = (
    spark.read.option("escape", '"')
    .option("multiline", True)
    .csv("/app/input/products_no_duplicates.csv", header=True, sep=",")
)
customers = (
    spark.read.option("escape", '"')
    .option("multiline", True)
    .csv("/app/input/customers.csv", header=True, sep=",")
)
transactions = (
    spark.read.option("escape", '"')
    .option("multiline", True)
    .csv("/app/input/transactions.csv", header=True, sep=",")
)
products.createOrReplaceTempView("products")
customers.createOrReplaceTempView("customers")
transactions.createOrReplaceTempView("transactions")

spark.sql(
    """
        select
        count(customer_id) as customers_no,
        first(brand) as brand,
        product_id
        from transactions t left join products p on t.product_id = p.asin
        group by product_id
        """
).write.csv("/app/output/6_join_output", mode="overwrite")
