#Create a SDP Streamin Table in SQL

from pyspark import pipelines as dp

#streamingtable franchise
@dp.table
def bronze_franchise():
    return spark.readStream.table("samples.bakehouse.sales_franchises")