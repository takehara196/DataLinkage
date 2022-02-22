# !/usr/bin/env python
# -*- coding:utf-8 -*-


from pymongo import MongoClient
import datetime
import pandas as pd
import ast
import numpy as np
import matplotlib.pyplot as plt
import os
import pprint


# DB接続
def _db_connect():
    client = MongoClient('mongodb://root:password123@localhost:27017')
    db = client.sample
    return db


# zips collection
def get_zips_table(db):
    collection = db.zips
    find = collection.find().limit(5)
    for doc in find:
        print(doc)




def main():
    db = _db_connect()
    get_zips_table(db)


if __name__ == '__main__':
    main()
