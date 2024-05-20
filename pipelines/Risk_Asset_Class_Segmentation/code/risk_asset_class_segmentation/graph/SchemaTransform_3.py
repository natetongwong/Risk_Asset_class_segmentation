from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *

def SchemaTransform_3(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(
        "LG_PRODUCT_L08_KEY",
        when((col("MONTH_KEY") > lit("201304")), lit("PFCRC")).otherwise(lit(""))
    )
