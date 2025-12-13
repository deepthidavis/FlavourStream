from pyspark import pipelines as dp
@dp.table
def bronze_sales():
    return spark.readStream.table("samples.bakehouse.sales_transactions")