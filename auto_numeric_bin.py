import pandas as pd
import matplotlib.pyplot as plt
import sys


def spark_auto_numeric_bin(df, column_name, method="auto"):
    """
    :param df: dataframe
    :param column_name:  columns name to draw
    :param method: 'auto', 'sturges', 'fd', 'doane', 'scott', 'rice' or 'sqrt'
    :return: None
    """

    pd_df = df.select(column_name).toPandas()
    plt.hist(pd_df[column_name], bins=method)
    plt.grid()
    # plt.show()
    plt.show(sys.stdout, format='svg')
