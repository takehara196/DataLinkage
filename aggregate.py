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


def read_excel():
    raw_data_df = pd.read_excel("rawdata/2021-11-09_promoV8NET-logdata.xlsx")
    # raw_data_df = pd.read_excel("rawdata/Engineer_V8LOG.xlsx")
    # raw_data_df = pd.read_excel("rawdata/Taiyoubuhinten_v8LOG.xlsx")
    # 月日, 時刻カラムは必ず使用する為, カラム使用の判別に用いない
    df = raw_data_df.drop(["月日", "時刻"], axis=1)
    return raw_data_df, df


def data_loss_rate(df):
    """
    カラム毎の欠損率確認
    60%以上NULLのカラムは使用しない
    """
    null_val = df.isnull().sum()
    percent = 100 * df.isnull().sum() / len(df)
    data_loss = pd.concat([null_val, percent], axis=1)
    data_loss_columns = data_loss.rename(
        columns={0: '欠損数', 1: '欠損率'})
    # 60％以上NULLのカラム
    drop_col_list = list(data_loss_columns.query('欠損率 > 60').index)  # 入力値の欠損率が55.6%であった為, 本データでは使用する判断
    return drop_col_list


def appearance_rate(df):
    """
    カラム毎の頻度確認
    すべて同一の値のカラムは使用しない
    """
    not_use_cols_list = []
    for col in df.columns:
        df_appearance_rate = pd.DataFrame(df[col].value_counts())
        # すべて同一要素のカラムを取得
        if df_appearance_rate.shape[0] == 1:
            not_use_cols_list.append(col)
    return not_use_cols_list


def drop_cols():
    """
    手動で削除するカラム
    設定ファイルなどしてもよい
    """
    drop_cols = [
        "グローバルIP"  # 54.250.12.5, 3.115.81.216の2種類のみで特に見ていない値の為
    ]
    return drop_cols


def _aggregate(raw_data_df, drop_col_list, not_use_cols_list):
    drop_col_list.extend(not_use_cols_list)
    # print(drop_col_list)
    df = raw_data_df.drop(drop_col_list, axis=1)
    df.to_csv("out/use_cols_df.csv", index=False)
    return df


def split_parameter_cols(df, raw_data_df):
    """
    パラメータの種別を抽出し, 各パラメータ毎のカラムを作成する
      parHinban1, parHinban2, parHinban3,...
      parJunsei1, parJunsei2, parJunsei3, ...
      parMakerNo1, parMakerNo2, parMakerNo3, ...
    """

    key_list = []
    for raw in df["パラメータ等"]:
        # 半角スペースとイコールの間の文字列を取得する
        # \w: アルファベット、アンダーバー、数字
        p = r"(\w+)="
        r = re.findall(p, raw)
        key_list.append(r)

        # 半角スペースを削除

        # パラメータ名=以降、次にパラメータリストにあるパラメータの前まで取得

    key_list = set(list(itertools.chain.from_iterable(key_list)))
    print(key_list)

    # df.to_csv("out/use_cols_df_.csv", index=False)

    # df['パラメータ等'] = df['パラメータ等'].str.replace(' ', '')

    # print(df['パラメータ等'][df['パラメータ等'].str.contains('parMakerNos')])
    for key in key_list:
        df['パラメータ等'] = df['パラメータ等'].str.replace(f'{key}', f',{key}')

    # 先頭の1文字を削除(先頭の,を削除)
    df["パラメータ等"] = df["パラメータ等"].str[1:]
    print(df["パラメータ等"])

    # カンマで分割する
    df_param = df['パラメータ等'].str.split(',', expand=True)
    # df_param.to_csv("df_param.csv", encoding="cp932")

    df_ = pd.DataFrame()
    for col in range(df_param.shape[1]):
        for key in key_list:
            df_[f'{key}_{col}'] = df_param[col].str.match(f'.*{key}=')
            # df_[f'{key}_{col}'] = df_param[col].str.match(f'.*{key}=*')
            df_.loc[df_[f'{key}_{col}'] == True, f'{key}_{col}'] = df_param[col]

    # FalseをNullに置換
    df_.replace(False, np.nan, inplace=True)
    # すべて欠損の列を削除
    df_.dropna(how='all', axis=1, inplace=True)
    # サフィックスを削除
    cols = df_.columns.str.replace('_.*', '', regex=True)
    # カラム名変更
    df_.columns = cols
    # 同一カラムが存在する場合は結合して一つのカラムにする
    identifier = df_.columns.to_series().groupby(level=0).transform('cumcount')

    df_.columns = df_.columns.astype('string') + "_" + identifier.astype('string')


    # Indexのサフィックスの数値から結合
    # サフィックスの数値が0であれば何もしない
    merge_col_list = []
    for col in df_.columns:
        # アンダーバーの数値が0なら何もしない
        if "_0" in col:
            pass
        # サフィックスの数値が0以外であれば数値回だけ結合する
        else:
            print(f"col: {col}")
            col = col.split('_')[0]
            print(f"col: {col}")
            merge_col_list.append(col)
    print(f"merge_col_list: {merge_col_list}")

    # print(f"df_before: {df_}")
    df_.to_csv("df_before.csv")

    # サフィックスの最大値を取得する
    suffix_max = max(list(identifier))
    print(list(identifier))
    # 動的にサフィックスの数値を取得
    sa = suffix_max
    for col in merge_col_list:
        for s in range(sa):
            try:
                a = str(s)
                b = str(s + 1)
                df_[f'{col}'] = df_[f'{col}_{a}'].str.cat(df_[f'{col}_{b}'], na_rep='')
                # print(df_[f'{col}'])
                # カラム削除
                df_.drop([f"{col}_{a}", f"{col}_{b}"], axis=1, inplace=True)
            except:
                pass

    print(f"df_after: {df_}")
    df_.to_csv("df_after.csv")

    print(cols)
    # サフィックスを削除
    cols = df_.columns.str.replace('_.*', '', regex=True)
    print(cols)
    # カラム名変更
    df_.columns = cols

    # 要素にある「カラム名=」を削除する。カラム名を変更した後に実行！
    for key in key_list:
        df_[key] = df_[key].replace(f'{key}=', '', regex=True)

    print(f"key_list: {key_list}")

    # # パラメータ名= を削除
    # for key in key_list:
    #     df_[f"{key}"] = df_[f"{key}"].str.replace(f"{key}=", "")
    #     print(df_[f"{key}"])

    df_.to_csv("df_.csv")
    split_cols = []
    # /区切りの要素が入るカラムを特定する
    for c in cols:
        try:
            print(f"param: {c}")
            df_c = df_[df_[c].str.contains("/", na=False)]
            # レコード数取得
            # 0レコード以外カラム名をリストに格納する
            if df_c.shape[0] == 0:
                pass
            else:
                split_cols.append(c)
                print(split_cols)
        except:
            pass

    for col in split_cols:
        print(f"{col}=")
        df_[f"{col}"] = df_[f"{col}"].fillna("/")
        df_[f"{col}"] = df_[f"{col}"].replace('/', ',', regex=True)
        df_[f"{col}"] = df_[f"{col}"].str.split(',')


    print(type(df_["ParamList"][0]))


    df_.to_csv("./out/df_regex.csv")


    # 可視化の際利用
    ans = sum(df_['ParamList'], [])
    c = collections.Counter(ans)
    df_c = pd.DataFrame.from_dict(c, orient='index').reset_index()
    print(df_c)

    df_.to_csv("out/param_cols.csv", index=False)

    # 月日, 時刻, 子ユーザカラムを付与しなおす
    df_ = pd.concat([raw_data_df[["月日", "時刻", "子ユーザ", "入力値", "コマンド", "受信サイズ", "受信数", "返却数"]], df_], axis=1)

    return df_


def delete_search_short_interval(df_):
    date_list = []
    for date in df_['月日']:
        date_list.append(date.strftime('%Y-%m-%d'))
    df_['月日'] = pd.DataFrame(date_list)

    time_list = []
    for time in df_['時刻']:
        time_list.append(time.strftime('%H:%M:%S'))
    df_['時刻'] = pd.DataFrame(time_list)
    # 月日 時刻を結合してdate列を作成
    df_['date'] = df_['月日'].str.cat(df_['時刻'], sep=' ')
    # 文字列 -> datetime
    df_['date'] = pd.to_datetime(df_['date'])
    # date昇順に並び替え
    df_.sort_values(by='date', ascending=True, inplace=True)
    # date列をindexにする
    # raw_data_df = raw_data_df.set_index("date")
    # print(df_.reset_index(drop=True))

    # 一定間隔の定義（秒数）
    split_interval = 1
    # 子ユーザリスト作成
    child_user_list = df_['子ユーザ'].unique()
    # 重複を除いたレコードを格納するデータフレーム
    duplicate_excluded_df = pd.DataFrame()

    for c in child_user_list:
        tmp_df = df_[df_["子ユーザ"] == c]
        # 最初のレコードの時刻をstartとする
        start = tmp_df.iloc[0]["date"]
        end = tmp_df.iloc[-1]["date"]
        # print(start)
        # print(end)

        # 差分を秒で取得、小数点切り捨て
        seconds_diff = math.floor((end - start).total_seconds())

        tmp_df.reset_index(drop=True, inplace=True)

        end = start + datetime.timedelta(seconds=seconds_diff)  # 秒後

        split_time = start + relativedelta(seconds=split_interval)
        # print(split_time)

        # end - startの秒数を算出し最大値とする
        counts = math.floor(seconds_diff / split_interval) + 1

        for c in range(counts):
            s = split_interval * c
            print(f"{s}-{s + split_interval - 1}sec")
            # split_time秒毎にスライスする
            mask = (tmp_df['date'] >= pd.Timestamp(start) + datetime.timedelta(seconds=s)) & \
                   (tmp_df['date'] <= pd.Timestamp(split_time) + datetime.timedelta(seconds=s))
            print(tmp_df[mask])
            # 最後のレコード（最後の検索）を取得する
            duplicate_excluded_df = duplicate_excluded_df.append(tmp_df[mask].tail(1), ignore_index=True)
    print(duplicate_excluded_df)
    duplicate_excluded_df.to_csv("out/duplicate_excluded_df_2021-11-09_promoV8NET-logdata_1sec.csv", index=False, encoding="cp932")
    # duplicate_excluded_df.to_csv("out/duplicate_excluded_df_Engineer_V8LOG.csv", index=False,encoding="cp932")
    # duplicate_excluded_df.to_csv("out/duplicate_excluded_df_Taiyoubuhinten_v8LOG.csv", index=False,encoding="cp932")
    return


def main():
    raw_data_df, df = read_excel()
    drop_col_list = data_loss_rate(df)
    not_use_cols_list = appearance_rate(df)
    df = _aggregate(raw_data_df, drop_col_list, not_use_cols_list)
    df_ = split_parameter_cols(df, raw_data_df)
    delete_search_short_interval(df_)


if __name__ == '__main__':
    main()
