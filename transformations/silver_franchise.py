from pyspark import pipelines as dp

# streamingtable silver_franchise
@dp.table(name="silver_franchise")
@dp.expect_or_drop("franchise_not_null", "franchiseID IS NOT NULL")
@dp.expect_or_drop("supplierID_not_null", "supplierID IS NOT NULL")
def silver_franchise():
    return spark.read.table("bronze_franchise")
