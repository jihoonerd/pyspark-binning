import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import seaborn as sns


def spark_auto_numeric_bin(df, column_name, cutoff=0.05, init_num_bin=10):

    column_bin = column_name + "_BIN"
    df_num_row = df.count()
    df_bin_column = df.select(column_name).dropna()
    df_bin_column_num_row = df_bin_column.count()
    print("{} rows with NULL are dropped.".format(df_num_row - df_bin_column_num_row))

    col_max = df.agg(F.max(df[column_name])).head()[0]
    col_min = df.agg(F.min(df[column_name])).head()[0]

    cutoff_bincount = cutoff * df_bin_column_num_row

    for num_bin in range(init_num_bin, 1, -1):
        print("Try to make {} bins...".format(num_bin))
        step = (col_max - col_min) / num_bin
        splits = [-float("inf")] + [col_min + i * step for i in range(1, num_bin)] + [float("inf")]
        bucketizer = Bucketizer(splits=splits, inputCol=column_name, outputCol=column_bin)
        bucketedData = bucketizer.transform(df)
        bucketed_value_count = bucketedData.groupBy(column_bin).count().orderBy(column_bin)
        print("Min / Max: ", [col_min, col_max])
        print("Splits: ", splits)
        if bucketed_value_count.agg(F.min(bucketed_value_count["count"])).head()[0] > cutoff_bincount:
            print("Success.")
            break
        else:
            print("Failed with {} bins".format(num_bin))
            continue

    print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits())-1))

    pd_df = bucketed_value_count.toPandas()
    sns.barplot(pd_df[column_bin], pd_df["count"])
    plt.grid()
    plt.show()