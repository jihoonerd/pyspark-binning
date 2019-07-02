import pyspark.sql.functions as F
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import seaborn as sns


def spark_auto_numeric_bin(df, column_name, cutoff=0.05, init_num_bin=10):

    column_bin = column_name + "_BIN"
    df_num_row = df.count()
    df_bin_column = df.select(column_name).dropna()     # Null값을 제외한 해당 열을 선택합니다.
    df_bin_column_num_row = df_bin_column.count()       # cutoff% 반영을 위해 해당 열 중 NULL이 아닌 갯수를 셉니다. 
    print("{} rows with NULL are dropped.".format(df_num_row - df_bin_column_num_row))  # Drop된 열 수를 출력합니다.

    col_max = df.agg(F.max(df[column_name])).head()[0]  # 해당 열의 최댓값을 구합니다.
    col_min = df.agg(F.min(df[column_name])).head()[0]  # 해당 열의 최솟값을 구합니다.

    cutoff_bincount = cutoff * df_bin_column_num_row    # Bin 최솟값이 이 값을 만족하지 못하면 줄어든 Bin의 갯수로 재시도합니다.

    for num_bin in range(init_num_bin, 1, -1):          # init_num_bin부터 1씩 줄여가며 Binning을 시도합니다.
        print("Try to make {} bins...".format(num_bin))
        step = (col_max - col_min) / num_bin            # Bin의 간격을 계산합니다.
        splits = [-float("inf")] + [col_min + i * step for i in range(1, num_bin)] + [float("inf")] # [-무한대 ~ x1, x1 ~ x2, ..., x9 ~ 무한대] 로 bin이 속하는 구간을 정의합니다.
        bucketizer = Bucketizer(splits=splits, inputCol=column_name, outputCol=column_bin)          # Bucketizer를 이용하여 column_name의 열을 Binnin합니다.
        bucketedData = bucketizer.transform(df)
        bucketed_value_count = bucketedData.groupBy(column_bin).count().orderBy(column_bin)
        print("Min / Max: ", [col_min, col_max])        # 해당 열의 Min / Max를 출력합니다.
        print("Splits: ", splits)                       # 각 Bin Index가 가리키는 구간을 보여줍니다.
        if bucketed_value_count.agg(F.min(bucketed_value_count["count"])).head()[0] > cutoff_bincount:  # 최소빈의 갯수가 cutoff_bincount보다 크다면 반복문을 벗어납니다.
            print("Success.")
            break
        else:                                           # 최소빈의 갯수가 cutoff_bincount보다 작다면 Bin 갯수를 줄여서 다시 시도합니다.
            print("Failed with {} bins".format(num_bin))
            continue

    print("Bucketizer output with %d buckets" % (len(bucketizer.getSplits())-1))

    pd_df = bucketed_value_count.toPandas()             # Binning된 테이블을 Pandas DataFrame으로 변환합니다.
    sns.barplot(pd_df[column_bin], pd_df["count"])      # Seabornn을 이용해 X축에 Bin Index, y축에 count가 표기되도록 그립니다. Bin Index는 splits에서 세부조건을 볼 수 있습니다.
    plt.grid()
    plt.show()