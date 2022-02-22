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
    # --- --- #
    raw_data_df = pd.read_csv('input/collection.csv')
    # fld_datetime(月日, 時刻)カラムは必ず使用する為, カラム使用の判別に用いない
    collection_df = raw_data_df.drop(['fld_datetime'], axis=1)
    return collection_df, raw_data_df


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
    print(f'50%以上NULLのカラム: {_50_percent_or_more_null_list}')

    # --- カラム毎の頻度確認 --- #
    same_value_column_list = []
    for col in collection_df.columns:
        dfappearance_rate = pd.DataFrame(collection_df[col].value_counts())
        # すべて同一要素のカラムを取得, すべて同一の値のカラムは使用しない
        if dfappearance_rate.shape[0] == 1:
            same_value_column_list.append(col)
    print(f'すべて同一要素のカラム: {same_value_column_list}')

    # --- 手動で削除判別するカラム --- #
    manual_determining_col_list = [
        "fld_globalip",
        "fld_localip",
        "fld_port"
    ]
    print(f'手動で削除判別するカラム : {manual_determining_col_list}')

    # --- 使用しないカラムを削除 --- #
    drop_cols_list.extend(_50_percent_or_more_null_list)
    drop_cols_list.extend(same_value_column_list)
    drop_cols_list.extend(manual_determining_col_list)
    print(f'使用しないカラム : {drop_cols_list}')

    collection_df.drop(drop_cols_list, axis=1, inplace=True)
    return collection_df


def split_fld_param(collection_df, raw_data_df):
    """
    パラメータの種別を抽出し, 各パラメータ毎のカラムを作成する
    """
    key_list = []
    for raw in collection_df["fld_param"]:
        # 半角スペースとイコールの間の文字列を取得する
        # \w: アルファベット、アンダーバー、数字
        p = r"(\w+)="
        r = re.findall(p, raw)
        key_list.append(r)
    key_list = set(list(itertools.chain.from_iterable(key_list)))

    # パラメータ名の前にカンマを付与(分割の為)
    for key in key_list:
        collection_df['fld_param'] = collection_df['fld_param'].str.replace(f'{key}', f',{key}')
    # 先頭の1文字を削除(先頭の,を削除)
    collection_df["fld_param"] = collection_df["fld_param"].str[1:]
    # 同一要素内のパラメータを1パラメータ1カラムに分割する
    fld_param_df = collection_df['fld_param'].str.split(',', expand=True)
    # カラム名_suffixにする
    df = pd.DataFrame()
    for col in range(fld_param_df.shape[1]):
        for key in key_list:
            df[f'{key}_{col}'] = fld_param_df[col].str.match(f'.*{key}=')
            df.loc[df[f'{key}_{col}'] == True, f'{key}_{col}'] = fld_param_df[col]
    # FalseをNullに置換
    df.replace(False, np.nan, inplace=True)
    # すべて欠損の列を削除
    df.dropna(how='all', axis=1, inplace=True)
    # サフィックスを削除
    cols = df.columns.str.replace('_.*', '', regex=True)
    # カラム名変更
    df.columns = cols
    # 同一カラム名が存在する場合は結合して一つのカラムにする
    identifier = df.columns.to_series().groupby(level=0).transform('cumcount')
    df.columns = df.columns.astype('string') + "_" + identifier.astype('string')

    # カラム名のsuffixの値により結合する
    merge_col_list = []
    for col in df.columns:
        # suffixの値が0であれば何もしない(結合の必要がない)
        if "_0" in col:
            pass
        # suffixの値が0以外であれば値だけ結合する
        else:
            col = col.split('_')[0]
            merge_col_list.append(col)

    # suffixの最大値を取得する
    suffix_max = max(list(identifier))
    #
    for col in merge_col_list:
        for s in range(suffix_max):
            try:
                df[f'{col}'] = df[f'{col}_{str(s)}'].str.cat(df[f'{col}_{str(s + 1)}'], na_rep='')
                # 結合前のカラム削除
                df.drop([f"{col}_{str(s)}", f"{col}_{str(s + 1)}"], axis=1, inplace=True)
            except:
                pass

    # suffixを削除
    cols = df.columns.str.replace('_.*', '', regex=True)
    # カラム名変更
    df.columns = cols

    # 要素にある「カラム名=」を削除, カラム名を変更した後に実行
    for key in key_list:
        df[key] = df[key].replace(f'{key}=', '', regex=True)

    split_cols = []
    # /区切りの要素が入るカラムを特定する
    for c in cols:
        try:
            dfc = df[df[c].str.contains("/", na=False)]
            # レコード数取得
            # 0レコード以外カラム名をリストに格納する
            if dfc.shape[0] == 0:
                pass
            else:
                split_cols.append(c)
        except:
            pass

    for col in split_cols:
        df[f"{col}"] = df[f"{col}"].fillna("/")
        df[f"{col}"] = df[f"{col}"].replace('/', ',', regex=True)
        df[f"{col}"] = df[f"{col}"].str.split(',')

    # 可視化の際利用
    ans = sum(df['ParamList'], [])
    c = collections.Counter(ans)
    dfc = pd.DataFrame.from_dict(c, orient='index').reset_index()

    # fld_datetimeを付与しなおす
    df = pd.concat([raw_data_df[["fld_datetime"]], df], axis=1)
    df.to_csv("output/param_cols.csv", index=False)
    print(df)

    return df


def main():
    collection_df, raw_data_df = read_collection_csv()
    collection_df = select_drop_cols(collection_df)
    split_fld_param(collection_df, raw_data_df)


if __name__ == '__main__':
    main()
