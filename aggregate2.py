# !/usr/bin/env python
# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import io
import re
import itertools


def aggregate():
    df = pd.read_csv("out/use_cols_df.csv")
    df_param = pd.read_csv("out/param_cols.csv")
    # パラメータ等列削除
    df.drop("パラメータ等", axis=1, inplace=True)
    df2 = pd.concat([df, df_param], axis=1)
    print(df2)


    df2.to_csv("out/df2.csv", index=False)


    return


def main():
    aggregate()



if __name__ == '__main__':
    main()
