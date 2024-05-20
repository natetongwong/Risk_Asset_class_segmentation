from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from risk_asset_class_segmentation.config.ConfigStore import *
from risk_asset_class_segmentation.functions import *

def Script_0(spark: SparkSession) -> DataFrame:
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import col
    # Define schema for the DataFrame
    schema = StructType([
            StructField("PRODUCT_TYPE_CODE", StringType(), True),
            StructField("CON_BUS_INDC", StringType(), True),
            StructField("MONTH_KEY", StringType(), True),
            StructField("LG_PRODUCT_L08_KEY", StringType(), True),
            StructField("REGLT_COUNTERPARTY_TYPE_CODE", StringType(), True),
            StructField("TCE", IntegerType(), True),
            StructField("BASEL_RETAIL_CORP_CODE", StringType(), True),
            StructField("SOURCE_SYSTEM_CODE", StringType(), True),
            StructField("CONSOL_ANNUAL_REVENUE", IntegerType(), True),
            StructField("ANZSIC_CODE", StringType(), True),
            StructField("OWNER_OCCUPIED_FLG", StringType(), True)

    ])
    # Sample data
    data = [('HL', 'C', '201305', '', '540', 50000, 'SMERET', 'CSH', 800000000, '7710', 'N'),
            ('NT', 'C', '201309', '', '540', 50000, 'SMECORP', 'OTH', 1000000000, '7714', 'N'),
            ('HL', 'B', '201305', '', '540', 50000, 'SMERET', 'CSH', 800000000, '7710', 'N'),
            ('NT', 'C', '201210', '', '540', 25000, 'SMECORP', 'AAA', 850000000, '7715', 'N'),
            ('NT', 'C', '201211', '', '510', 40000, 'SMECORP', 'OTH', 750000001, '7716', 'N'),
            ('NT', 'C', '201212', '', '534', 50000, 'SMECORP', 'OTH', 800000000, '7717', 'N'),
            ('NT', 'C', '201201', '', '530', 75000, 'SMECORP', 'OTH', 550000000, '7728', 'N'),
            ('NT', 'C', '201201', '', '538', 75000, 'SMECORP', 'OTH', 900000000, '7718', 'N'),
            ('NT', 'B', '201201', '', '540', 75000, 'SMECORP', 'OTH', 850000000, '7718', 'N'),
            ('NT', 'B', '201201', '', '540', 75000, 'SMECORP', 'OTH', 550000000, '7718', 'N')]
    # Create DataFrame
    out0 = spark.createDataFrame(data, schema)

    return out0
