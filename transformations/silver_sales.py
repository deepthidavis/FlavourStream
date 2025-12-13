from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table(name="silver_sales")
@dp.expect_or_drop("transaction_id_not_null","transactionID IS NOT NULL")
@dp.expect_or_drop("quantity_greater_zero","quantity>0")
def silver_sale():
    df=spark.read.table("bronze_sales")
    return df.withColumn("txn_date",to_date(col("dateTime")))
            