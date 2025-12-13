from pyspark import pipelines as dp


#streaming table suppliers
@dp.table
def bronze_suppliers():
    return spark.readStream.table("samples.bakehouse.sales_suppliers")