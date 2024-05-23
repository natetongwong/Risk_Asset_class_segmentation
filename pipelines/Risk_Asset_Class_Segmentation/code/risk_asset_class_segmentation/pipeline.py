from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *
from prophecy.utils import *
from risk_asset_class_segmentation.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Sample_Data = Sample_Data(spark)
    df_Add_Rule = Add_Rule(spark, df_Sample_Data)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Risk_Asset_Class_Segmentation")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Risk_Asset_Class_Segmentation")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Risk_Asset_Class_Segmentation", config = Config)(
        pipeline
    )

if __name__ == "__main__":
    main()
