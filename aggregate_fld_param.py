# !/usr/bin/env python
# -*- coding:utf-8 -*-

import pandas as pd
import numpy as np
import datetime
import io
import re
import itertools
import collections
import matplotlib.pyplot as plt
from datetime import timedelta
import math
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient


def read_collection_csv():
    # ---  --- #
    collection_df = pd.read_csv('collection.csv')
    # fld_datetime(月日, 時刻)カラムは必ず使用する為, カラム使用の判別に用いない
    collection_df = collection_df.drop(['fld_datetime'], axis=1)
    return collection_df


def select_drop_cols(collection_df):
    drop_cols_list = []

    # --- カラム毎の欠損率確認 --- #
    null_val = collection_df.isnull().sum()
    percent = 100 * collection_df.isnull().sum() / len(collection_df)
    data_loss = pd.concat([null_val, percent], axis=1)
    data_loss_columns = data_loss.rename(
        columns={0: '欠損数', 1: '欠損率'})
    # 50%以上NULLのカラムは使用しないこととする
    _50_percent_or_more_null_list = list(data_loss_columns.query('欠損率 >= 50').index)
    print(f"50%以上NULLのカラム: {_50_percent_or_more_null_list}")

    # --- カラム毎の頻度確認 --- #
    same_value_column_list = []
    for col in collection_df.columns:
        df_appearance_rate = pd.DataFrame(collection_df[col].value_counts())
        # すべて同一要素のカラムを取得, すべて同一の値のカラムは使用しない
        if df_appearance_rate.shape[0] == 1:
            same_value_column_list.append(col)
    print(f"すべて同一要素のカラム: {same_value_column_list}")

    # --- 手動で削除判別するカラム --- #
    manual_determining_col_list = [
        "fld_globalip",
        "fld_localip",
        "fld_port"
    ]
    print(f"手動で削除判別するカラム : {manual_determining_col_list}")

    # --- 使用しないカラムを削除 --- #
    drop_cols_list.extend(_50_percent_or_more_null_list)
    drop_cols_list.extend(same_value_column_list)
    drop_cols_list.extend(manual_determining_col_list)
    print(drop_cols_list)

    collection_df.drop(drop_cols_list, axis=1, inplace=True)
    collection_df.to_csv("out/collection_df.csv", index=False)

    return collection_df


def split_fld_param(collection_df):
    """
    パラメータの種別を抽出し, 各パラメータ毎のカラムを作成する
    """
    key_list = []
    for raw in collection_df['fld_param']:
        # 半角スペースとイコールの間の文字列を取得する
        # \w: アルファベット、アンダーバー、数字
        p = r"(\w+)="
        r = re.findall(p, raw)
        key_list.append(r)
    key_list = set(list(itertools.chain.from_iterable(key_list)))
    # print(key_list)

    collection_df['fld_param'] = collection_df['fld_param'].str.replace(' ', '')
    for key in key_list:
        collection_df['fld_param'] = collection_df['fld_param'].str.replace(f'{key}', f',{key}')
    # 先頭の1文字を削除(先頭の,を削除)
    collection_df['fld_param'] = collection_df['fld_param'].str[1:]
    # カンマで分割する
    tmp_fld_param_df = collection_df['fld_param'].str.split(',', expand=True)
    fld_param_df = pd.DataFrame()
    for col in range(tmp_fld_param_df.shape[1]):
        for key in key_list:
            fld_param_df[f'{key}_{col}'] = tmp_fld_param_df[col].str.match(f'.*{key}=')
            # df_[f'{key}_{col}'] = df_param[col].str.match(f'.*{key}=*')
            fld_param_df.loc[fld_param_df[f'{key}_{col}'] == True, f'{key}_{col}'] = tmp_fld_param_df[col]
    # FalseをNullに置換
    fld_param_df.replace(False, np.nan, inplace=True)
    # すべて欠損の列を削除
    fld_param_df.dropna(how='all', axis=1, inplace=True)
    # サフィックスを削除
    cols = fld_param_df.columns.str.replace('_.*', '', regex=True)
    # カラム名変更
    fld_param_df.columns = cols
    # print(fld_param_df)
    # 同一カラムが存在する場合は結合して一つのカラムにする
    identifier = fld_param_df.columns.to_series().groupby(level=0).transform('cumcount')
    fld_param_df.columns = fld_param_df.columns.astype('string') + "_" + identifier.astype('string')
    fld_param_list = fld_param_df.columns.values.tolist()
    print(fld_param_df)

    col_list = []
    for col in fld_param_list:
        target = '(_.*)'
        idx = col.find(target)
        r = col[:idx]
        col_in = [c for c in fld_param_list if r in c]
        col_list.append(col_in)

    # 重複リストは削除
    def get_unique_list(seq):
        seen = []
        return [x for x in seq if x not in seen and not seen.append(x)]

    for col_list in get_unique_list(col_list):
        tmp_df = pd.DataFrame()
        print(len(col_list))
        for c in range(len(col_list)):
            tmp_df = fld_param_df[col_list].iloc[:, c:c+1]
            # tmp_df = tmp_df.append(fld_param_df[[c]])
            # tmp_df = fld_param_df[c]
        print(tmp_df)
        print('-----')

    # tmp_df = pd.DataFrame()
    # for col_list in get_unique_list(col_list):
    #     # print(col_list)
    #     # 要素が複数の場合,カンマ区切りで結合する
    #     if len(col_list) == 1:
    #         # print(l)
    #         pass
    #     else:
    #         i = 0
    #         for c in col_list:
    #             print(f'c: {c}')
    #             i += 1
    #             f = fld_param_df[[c]]
    #             print(f'f: {f}')
    #             if i <= len(col_list):
    #                 tmp_df = f + "," + fld_param_df[[col_list[i+1]]]
    #                 print(f'tmp_df: {tmp_df}')
    #             else:
    #                 pass
    #         pass


def main():
    collection_df = read_collection_csv()
    collection_df = select_drop_cols(collection_df)
    split_fld_param(collection_df)


if __name__ == '__main__':
    main()
