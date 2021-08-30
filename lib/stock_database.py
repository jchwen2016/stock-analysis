# !/usr/bin/env python
# coding: utf-8
# @Time    : 2021/6/21 10:04
# @Author  : Jia chuanwen
# @File    : stock_database.py


import pandas as pd
import datetime
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

    def write_data_by_stock(self, df):
        if df.shape[0] == 0:
            return
        ts_code = df.loc[0, 'ts_code']
        stock_code, exchange = ts_code.split('.')
        table_name = exchange.lower() + stock_code
        df.to_sql(table_name, self.engine_ts, index=False, if_exists='append', chunksize=5000)

    def write_data_by_date(self):
        pass

    def read_data(self, ts_code, start_date=None, end_date=None):
        stock_code, exchange = ts_code.split('.')
        table_name = exchange.lower() + stock_code

        if start_date is None and end_date is None:
            sql = """SELECT * FROM {} """.format(table_name)
        if start_date is not None and end_date is None:
            sql = """SELECT * FROM {} where trade_date>{}""".format(table_name, start_date)
        if start_date is None and end_date is not None:
            sql = """SELECT * FROM {} where trade_date<{}""".format(table_name, end_date)
        if start_date is not None and end_date is not None:
            sql = """SELECT * FROM {} where trade_date>{} and trade_date<{}""".format(table_name, start_date, end_date)
        try:
            stock_data = pd.read_sql_query(sql, self.engine_ts)
        except:
            stock_data = None
        return stock_data

    def write_stock_list(self, df):
        today = datetime.date.today()
        today = today.strftime('%Y%m%d')
        table_name = 'stock_list_{}'.format(today)
        df.to_sql(table_name, self.engine_ts, index=False, if_exists='replace', chunksize=5000)
