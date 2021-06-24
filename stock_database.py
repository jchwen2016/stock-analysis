# !/usr/bin/env python
# coding: utf-8
# @Time    : 2021/6/21 10:04
# @Author  : Jia chuanwen
# @File    : stock_database.py


import pandas as pd
from sqlalchemy import create_engine


class StockDatabase(object):
    def __init__(self, database_config):
        self.engine_ts = create_engine('{}://{}:{}@{}:{}/{}?charset=utf8&use_unicode=1'.format(
                database_config['database_type'],
                database_config['user'],
                database_config['password'],
                database_config['ip'],
                database_config['port'],
                database_config['database_name'])
            )
        return

    def write_data_by_stock(self,engine_ts, df):
        ts_code = df.loc[0, 'ts_code']
        stock_code, exchange = ts_code.split('.')
        table_name = exchange.lower() + stock_code
        df.to_sql(table_name, engine_ts, index=False, if_exists='append', chunksize=5000)

    def write_data_by_date(self):
        pass

    def read_data(self, engine_ts):
        sql = """SELECT * FROM stock_basic LIMIT 20"""
        df = pd.read_sql_query(sql, engine_ts)
        return df