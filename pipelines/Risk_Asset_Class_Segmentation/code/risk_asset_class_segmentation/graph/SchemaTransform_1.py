from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *

def SchemaTransform_1(spark: SparkSession, Reformat_1: DataFrame) -> DataFrame:
    return Reformat_1.select(expr("*"), Risk_Asset_Class_Segmentation())
