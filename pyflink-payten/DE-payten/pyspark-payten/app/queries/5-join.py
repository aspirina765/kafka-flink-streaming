from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = (
    SparkSession.builder.master("spark://spark-master:7077")
    .appName("5-join")
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
        transaction_id,
        product_id,
        transaction_amount,
        transaction_date,
        c.customer_id,
        category,
        c.title as ctitle,
        brand,
        `rank`, --- escaped
        main_cat,
        first_name,
        last_name,
        gender,
        ssn,
        credit_card,
        credit_card_provider,
        birth_date,
        start_date,
        office,
        organization,
        salary,
        bonus,
        accured_holidays
        from transactions t left join products p on t.product_id=p.asin
        left join customers c on t.customer_id=c.customer_id
   """
).write.csv("/app/output/5_join_output", mode="overwrite")
