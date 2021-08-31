import datetime
import pandas as pd
import tushare as ts


def add_company_info():
    pass


def rank_stock_by_increase(data_former, data_latter, ignore=False, drop_nan=False):
    date_former = data_former['trade_date'].iloc[0]
    date_latter = data_latter['trade_date'].iloc[0]

    data_former = data_former[['ts_code', 'close']]
    data_latter = data_latter[['ts_code', 'close']]
    data_former.rename(columns={'close': date_former}, inplace=True)
    data_latter.rename(columns={'close': date_latter}, inplace=True)

    data_latter = data_latter.join(data_former.set_index('ts_code'), on='ts_code')

    increase = data_latter[date_latter] - data_latter[date_former]
    increase_rate = round((increase / data_latter[date_latter]) * 100, 2)
    data_latter["涨幅_排行"] = increase_rate
    data_latter.sort_values(by=["涨幅_排行"], ascending=False, inplace=True)
    if drop_nan:
        data_latter = data_latter.dropna(axis=0, how='any')
    return data_latter


def cal_increase(ranked_data, data_after, drop_nan):
    for date, data in data_after.items():
        data = data[['ts_code', 'close']]
        data.rename(columns={'close': date}, inplace=True)
        ranked_data = ranked_data.join(data.set_index('ts_code'), on='ts_code')

        increase = ranked_data[date] - ranked_data.iloc[:, 1]
        increase_rate = round((increase / ranked_data.iloc[:, 1]) * 100, 2)
        ranked_data["涨幅_{}".format(date)] = increase_rate
    if drop_nan:
        ranked_data = ranked_data.dropna(axis=0, how='any')
    return ranked_data


if __name__ == '__main__':
    pro = ts.pro_api()
    all_stock_info = pro.stock_basic(exchange='',
                                     list_status='L',
                                     fields='ts_code,symbol,name,area,industry,list_date')
    all_stock_info.index = all_stock_info['ts_code']
    all_company_info = pro.stock_company(fields='ts_code,reg_capital,setup_date,province,'
                                                'introduction,main_business,business_scope')
    all_company_info.index = all_company_info['ts_code']

    anchor_day = '2021-06-25'
    anchor_day = datetime.date(*map(int, anchor_day.split('-')))
    anchor_day_next = anchor_day + datetime.timedelta(days=1)
    anchor_before = anchor_day + datetime.timedelta(days=-40)
    anchor_after = anchor_day + datetime.timedelta(days=40)
    anchor_before = anchor_before.strftime('%Y%m%d')
    anchor_after = anchor_after.strftime('%Y%m%d')
    anchor_day = anchor_day.strftime('%Y%m%d')
    anchor_day_next = anchor_day_next.strftime('%Y%m%d')

    trade_date_before = pro.trade_cal(exchange='', start_date=anchor_before, end_date=anchor_day, is_open='1')
    trade_date_after = pro.trade_cal(exchange='', start_date=anchor_day_next, end_date=anchor_after, is_open='1')

    data_before = {}
    for date in trade_date_before['cal_date']:
        data = pro.daily(trade_date=date)
        data_before[date] = data
    data_after = {}
    for date in trade_date_after['cal_date']:
        data = pro.daily(trade_date=date)
        data_after[date] = data
    print('已获取全部数据')
    anchor_date = trade_date_before['cal_date'].iloc[-1]
    anchor_data = data_before[anchor_date]
    with pd.ExcelWriter(r'./max_increase_analysis/{}.xlsx'.format(anchor_date)) as writer:
        day_num_before = trade_date_before.shape[0]
        for index in range(2, day_num_before):
            print(index, day_num_before)
            date = trade_date_before['cal_date'].iloc[-index]
            data = data_before[date]
            ranked_result = rank_stock_by_increase(data, anchor_data, ignore=True, drop_nan=True)
            result = cal_increase(ranked_result, data_after, drop_nan=True)
            result.to_excel(writer, sheet_name='{}天榜'.format(str(index-1)), index=False, encoding='utf-8-sig')
