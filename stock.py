# !/usr/bin/env python
# coding: utf-8
# @Time    : 2021/6/21 10:04
# @Author  : Jia chuanwen
# @File    : stock.py


import time
import pandas as pd
import tushare as ts


class Stock(object):
    def __init__(self, stock_dataframe):
        """
        股票数据类
        :param stock_dataframe:
        """
        stock_dataframe.sort_values(by=['trade_date'], inplace=True)
        stock_dataframe.index = stock_dataframe.index.sort_values()
        self.data = stock_dataframe
        self.start_date = stock_dataframe.iloc[0]['trade_date']
        self.end_date = stock_dataframe.iloc[-1]['trade_date']
        self.code = stock_dataframe.iloc[0]['ts_code']
        self.row = stock_dataframe.shape[0]
        self.col = stock_dataframe.shape[1]

