import pandas as pd
from sqlalchemy import Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT
from dateutil.relativedelta import relativedelta

def clear_old_data(engine) :
    clear_industry_label_daily(engine, days = 90)
    clear_theme_label_daily(engine, days = 90)


def clear_industry_label_daily(engine, days = 90) :
    
    data = pd.read_sql('select * from industry_label_daily', con = engine)

    to_date = data['날짜'].max()
    to_date = pd.to_datetime(to_date)
    from_date = to_date - relativedelta(days = days)
    from_date = from_date.strftime('%Y-%m-%d')
    data = data[from_date <= data['날짜']]
    data = data.drop_duplicates()
    data.to_sql(
            'industry_label_daily',
            con = engine,
            if_exists='replace',
            index = False,
            dtype = {
                    '점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '업종점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
            }
    )
    
def clear_theme_label_daily(engine, days = 90) :
    
    data = pd.read_sql('select * from theme_label_daily', con = engine)

    to_date = data['날짜'].max()
    to_date = pd.to_datetime(to_date)
    from_date = to_date - relativedelta(days = days)
    from_date = from_date.strftime('%Y-%m-%d')
    data = data[from_date <= data['날짜']]
    data = data.drop_duplicates()
    data.to_sql(
            'theme_label_daily',
            con = engine,
            if_exists='replace',
            index = False,
            dtype = {
                    '점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '테마점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
            }
    )