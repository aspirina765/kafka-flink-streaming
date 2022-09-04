from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("8-join")
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
        CONCAT(first(first_name), ' ', first(last_name))  as full_name,
        CONCAT_WS(";",collect_set(p.brand)) as product_brand,
        CONCAT_WS(";",collect_set(p.main_cat)) as main_cat,
        CONCAT_WS(";",collect_list(transaction_id)) as transactions,
        MIN(transaction_date) as first_transaction_date,
        avg(cast(transaction_amount as double)) as avg_transaction,
        min(cast(transaction_amount as double)) as min_transaction,
        max(cast(transaction_amount as double)) as max_transaction,
        avg(cast(salary as double)) as avg_salary,
        avg(cast(bonus as double)) as avg_bonus
        from transactions t left join customers c
        on t.customer_id=c.customer_id
        left join products p on t.product_id = p.asin
        group by t.customer_id"""
).write.csv("/app/output/8_join_output", mode="overwrite")
