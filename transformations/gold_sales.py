from pyspark import pipelines as dp
from pyspark.sql.functions import sum as sum, col

@dp.table(
    name="gold_flagship_products",
    comment="Top 10 flagship products by total sales using Silver tables"
)
def gold_flagship_products():

    sales = dp.read("silver_sales").alias("T")
    franchise = dp.read("silver_franchise").alias("F")
    supplier = dp.read("silver_supplier").alias("S")

    joined_df = (
        sales.join(franchise, col("T.franchiseId") == col("F.franchiseID"), "left")
             .join(supplier, col("F.supplierID") == col("S.supplierID"), "inner")
    )

    result_df = (
        joined_df
        .groupBy(
            col("F.name").alias("franchise_name"),
            col("F.city").alias("franchise_city"),
            col("F.district").alias("franchise_district"),
            col("S.ingredient").alias("supplier_ingredient")
        )
        .agg(sum("totalPrice").alias("total"))
        .orderBy(col("total").desc())
        .limit(10)
    )

    return result_df

