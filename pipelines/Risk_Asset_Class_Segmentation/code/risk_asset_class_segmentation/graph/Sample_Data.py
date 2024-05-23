from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *

def Sample_Data(spark: SparkSession) -> DataFrame:
    return spark.read.table("`westpac`.`raw`.`AssetClassSegmentation_sampledata`")
