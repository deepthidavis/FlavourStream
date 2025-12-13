from pyspark import pipelines as dp

#streamingtable silver_supplier
@dp.table(name="silver_supplier")
@dp.expect_or_drop("supplier_not_null","supplierID IS NOT NULL")
@dp.expect_or_drop("approved_status","approved='Y'")
def silver_franchise():
    return spark.read.table("bronze_suppliers")