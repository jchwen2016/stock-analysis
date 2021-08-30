# !/usr/bin/env python
# coding: utf-8
# @Time    : 2021/6/21 10:04
# @Author  : Jia chuanwen
# @File    : stock_get.py


import time
import tushare as ts


class StockGet(object):
    def __init__(self, adj='qfq', ma=None, factors=None):
        self.adj = adj
        if ma is None:
            self.ma = [5, 10, 20, 30]
        else:
            self.ma = ma
        if factors is None:
            self.factors = ['tor', 'vr']    # 支持 tor换手率 vr量比
        else:
            self.factors = factors
        self.pro = ts.pro_api()
        return

    def get_data_by_stock(self, ts_code, start_date=None, end_date=None, adj=None, ma=None, factors=None):
        if adj is None:
            adj = self.adj
        if ma is None:
            ma = self.ma
        if factors is None:
            factors = self.factors
        for _ in range(3):
            try:
                stock_req = ts.pro_bar(ts_code=ts_code, adj=adj, start_date=start_date, end_date=end_date,
                                ma=ma, factors=factors)
            except:
                print('{} 第{}次请求失败，正在重试~'.format(ts_code, _))
                time.sleep(1)
            else:
                return stock_req
        print('{} 请求失败~'.format(ts_code))
        return None

    def get_data_by_date(self, trade_date='20180810'):
        pass

    def get_stock_list(self):
        # 查询当前所有正常上市交易的股票列表

        for _ in range(3):
            try:
                data = self.pro.stock_basic(exchange='', list_status='L')
            except:
                print('获取目前正常上市交易的股票列表， 第{}次请求失败，正在重试~'.format(_))
                time.sleep(1)
            else:
                return data
        print('获取目前正常上市交易的股票列表失败~')
        return None

