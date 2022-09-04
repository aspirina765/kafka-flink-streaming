from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("7-join")
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
        first(c.first_name),
        first(c.last_name),
        avg(transaction_amount) as avg_ta,
        avg(salary+bonus) as avg_income,
        avg(salary+bonus) - avg(transaction_amount) as spending
        from transactions as t left join customers
        as c on t.customer_id = c.customer_id
        group by c.customer_id"""
).write.csv("/app/output/7_join_output", mode="overwrite")
