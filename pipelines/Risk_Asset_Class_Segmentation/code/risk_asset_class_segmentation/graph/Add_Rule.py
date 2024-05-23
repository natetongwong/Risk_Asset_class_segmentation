from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *

def Add_Rule(spark: SparkSession, in0: DataFrame) -> DataFrame:
    return in0.withColumn(get_alias(Risk_Asset_Class_Segmentation()), Risk_Asset_Class_Segmentation())
