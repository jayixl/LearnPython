import sys
from datetime import date

import pandas as pd

import DateTimeUtils

sys.path.insert(1, '..\\MySql')
import MySqlUtils

_observe_range = 30

def get_fund_flow_data(universe, days):

    '''
    training_days must be confirmed to be existing days, and any one could be filtered if it is invalid
    '''

    data_all = {}
    start_day = days[0]
    end_day = days[-1]

    for symbol in universe:
        query_res = MySqlUtils.fund_flow_query(symbol, start_day, end_day)
        data = [[col for col in row] for row in query_res]
        df = pd.DataFrame(data, index = [date.strftime(pd.to_datetime(t[1]), '%Y-%m-%d') for t in data], columns = MySqlUtils.fundflow_columns)

        # we can't get column name info by lambda
        #df = df.apply(lambda col: pd.to_numeric(col, errors='coerce'))
        #df = df.apply(lambda col: col.astype('float'))
        for col_name in df.columns:
            if (col_name != 'symbol' and col_name != 'date' and col_name != 'name'):
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                df[col_name] = df[col_name].interpolate()
            # if (col_name == 'date'):
            #     df[col_name] = pd.to_datetime(df[col_name]).applay(lambda x: date.datetime.isoformat(x))

        # df2 = get_single_stock_reg_data(df, _factors, training_days, _observe_shift)
        df2 = df
        if df2 is not None:
            data_all[symbol] = df2

    return data_all


def test():
    universe = ['000001', '000002']
    day = '2014-12-01'
    data_all = get_fund_flow_data(universe, ['2017-05-01', '2017-07-01'])
    data01 = data_all['000002']
    print data01
    observe_days = DateTimeUtils.get_prev_fund_flow_days('2017-05-30', 3)
    print observe_days[0]
    print observe_days[-1]
    print data01.loc[observe_days[0]:observe_days[-1]]

test()
