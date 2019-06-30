# pyspark-binning

## Example code

```python
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

```

## Example output
```bash
177 rows with NULL are dropped.
Try to make 10 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 8.378, 16.336000000000002, 24.294000000000004, 32.252, 40.21, 48.168000000000006, 56.126000000000005, 64.084, 72.042, inf]
                                                                                Failed with 10 bins
Try to make 9 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 9.262222222222222, 18.104444444444447, 26.94666666666667, 35.78888888888889, 44.63111111111111, 53.473333333333336, 62.31555555555556, 71.15777777777778, inf]
                                                                                Failed with 9 bins
Try to make 8 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 10.3675, 20.315, 30.262500000000003, 40.21, 50.1575, 60.105000000000004, 70.0525, inf]
                                                                                Failed with 8 bins
Try to make 7 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 11.788571428571428, 23.15714285714286, 34.52571428571429, 45.894285714285715, 57.26285714285714, 68.63142857142857, inf]
                                                                                Failed with 7 bins
Try to make 6 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 13.683333333333334, 26.94666666666667, 40.21, 53.473333333333336, 66.73666666666666, inf]
                                                                                Failed with 6 bins
Try to make 5 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 16.336000000000002, 32.252, 48.168000000000006, 64.084, inf]
                                                                                Failed with 5 bins
Try to make 4 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 20.315, 40.21, 60.105000000000004, inf]
                                                                                Failed with 4 bins
Try to make 3 bins...
Min / Max:  [0.42, 80.0]
Splits:  [-inf, 26.94666666666667, 53.473333333333336, inf]
                                                                                Success.
Bucketizer output with 3 buckets
```
