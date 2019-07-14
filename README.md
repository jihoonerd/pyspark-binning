# pyspark-binning

## Example code

```python
import pyspark
import pyspark.sql.functions as F
from auto_numeric_bin import spark_auto_numeric_bin

data_path = "./data/titanic_train.csv"
column_name = "Age"

spark = pyspark.sql.SparkSession.builder.master("local").appName("test").getOrCreate()

df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true") \
    .load(data_path)

# You can choose one of: 'auto', 'sturges', 'fd', 'doane', 'scott', 'rice' or 'sqrt'
spark_auto_numeric_bin(df, column_name, method='auto')

```