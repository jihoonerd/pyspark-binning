import pyspark
import pyspark.sql.functions as F
from auto_numeric_bin import spark_auto_numeric_bin

data_path = "./data/titanic_train.csv"
column_name = "Age"
column_bin = column_name + "_BIN"

spark = pyspark.sql.SparkSession.builder.master("local").appName("test").getOrCreate()

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true") \
    .load(data_path)

spark_auto_numeric_bin(df, column_name, cutoff=0.05, init_num_bin=10)
