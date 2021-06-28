# !/usr/bin/env python
# coding: utf-8
# @Time    : 2021/6/21 10:04
# @Author  : Jia chuanwen
# @File    : generate_data.py
"""
生成训练数据，以过去30日交易日数据作为一条记录，标签为未来10天内是否存在涨幅超过10%
"""
import pandas as pd
import numpy as np
from stock_database import StockDatabase
from stock import Stock

if __name__ == '__main__':
    database_config = {
        'database_type': 'mysql',
        'user': 'root',
        'password': 'jchwen',
        'ip': '127.0.0.1',
        'port': '3306',
        'database_name': 'production_stock'
    }
    stock_database = StockDatabase(database_config)
    stock = stock_database.read_data('sh601111', start_date='20000110')
    stock = Stock(stock)
    print(stock.data)
    row = stock.row
    col = stock.col
    col_value = []
    for i in range(col):
        if isinstance(stock.data.iloc[40][i], np.float64):
            col_value.append(stock.data.columns[i])
    X_data = []
    Y_data = []
    trade_dates = []
    for i in range(60, row-9):
        data = stock.data.iloc[i-30:i]
        high = stock.data.iloc[i:i+10][['high']]
        high_max = np.float64(high.max())
        price = stock.data.iloc[i]['open']
        trade_date = stock.data.iloc[i]['trade_date']
        increase_rate = (high_max - price) / price
        label = 1 if increase_rate > 0.1 else 0
        data = data[col_value]
        data = np.array(data).reshape(1, -1)
        data = np.squeeze(data, 0)
        X_data.append(data)
        Y_data.append(label)
        trade_dates.append(trade_date)
    X_data = np.array(X_data)
    Y_data = np.expand_dims(np.array(Y_data), 1)
    print(X_data.shape)
    print(Y_data.shape)
    dataset = np.concatenate((X_data, Y_data), axis=1)
    dataset = pd.DataFrame(dataset, index=trade_dates)
    dataset.to_csv('{}.csv'.format(stock.code))


