from pyspark.sql import SparkSession
import sys
import datetime


# Input and Output Arguments

Input = sys.argv[1]
Output = sys.argv[2]

# Creating Spark Session
todayDate=str(datetime.datetime.now().strftime("%Y-%m-%d"))
f_date=todayDate[0:4]+todayDate[5:7]+todayDate[8:10]
spark = SparkSession.builder.\
    appName("AnalyticsApp").getOrCreate()


dfInput = spark.read.format("com.databricks.spark.csv").\
    option("header", "true").\
    option("treatEmptyValuesAsNulls", "true").\
    option("inferSchema", "true").\
    option("quote", "\"").\
    option("multiLine", "true").\
    option("spark.read.simpleMode", "true").\
    option("useHeader", "true").\
    load(Input)

#dfInput.coalesce(1).write.mode('append').format('parquet').save(Output)
output_folder = Output+'test_'+f_date
dfInput.coalesce(1).write.mode('append').format('csv').save(output_folder)
