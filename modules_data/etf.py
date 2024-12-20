import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def create_etf_base_table(engine):

    etf_name = fdr.StockListing('ETF/KR')[['Symbol', 'Name']]
    etf_name.columns = ['etf_code', 'etf_name']

    data = pd.read_sql('SELECT * FROM new_data', con = engine)
    data.columns = ['etf_code', 'stock_code', 'stock_name', 'recent_quantity', 'recent_amount', 'recent_ratio']

    data2 = pd.read_sql('SELECT * FROM old_data', con = engine)
    data2.columns = ['etf_code', 'stock_code', 'stock_name', 'past_quantity', 'past_amount', 'past_ratio']

    data = data.merge(data2, how='outer', on=['etf_code', 'stock_code'])
    data = data.merge(etf_name, how='left', on='etf_code')

    data['stock_name_x'] = data['stock_name_x'].fillna(data['stock_name_y'])
    data.drop(['stock_name_y'], axis=1, inplace=True)
    data = data.rename(columns={'stock_name_x': 'stock_name'})
    data.fillna(0, inplace=True)
    data['etf_name'] = data['etf_name'].astype(str)

    data = data[['etf_code', 'etf_name', 'stock_code', 'stock_name',
                    'recent_quantity', 'recent_amount', 'recent_ratio',
                    'past_quantity', 'past_amount', 'past_ratio']]
    data['diff_ratio'] = data['recent_ratio'] - data['past_ratio']

    data.to_sql('etf_base_table', con = engine, if_exists='replace', index=False,
                dtype={
                    'etf_code': String(12),
                    'etf_name': String(50),
                    'stock_code': String(12),
                    'stock_name': String(50),
                    'recent_quantity': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'recent_amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'recent_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'past_quantity': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'past_amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'past_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'diff_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                })

    return data

def create_etf_report_table(research, engine):

    def str_to_float(s):
        if s == "":
            return None
        elif s == "없음":
            return None
        elif s == None:
            return None
        try:
            return float(s)
        except ValueError:
            return None
        

    research['목표가'] = research['목표가'].apply(str_to_float)
    target_price = research.groupby('종목코드')['목표가'].mean().reset_index()

    idx = research.groupby('종목코드')['nid'].idxmax()
    research = research.loc[idx, :]

    research = research.drop('목표가', axis=1).merge(target_price, how='outer', on='종목코드')
    research.drop('nid', axis=1, inplace=True)

    research.columns = ['stock_name', 'stock_code', 'report_title', 'report_opinion', 'report_pubdate',
                    'report_researcher',
                    'report_link', 'stock_target_price']
    research = research[['stock_name', 'stock_code', 'stock_target_price',
                    'report_title', 'report_opinion', 'report_pubdate', 'report_researcher', 'report_link']]

    research.to_sql('etf_deposit_detail', con = engine, if_exists='replace', index=False,
                dtype={
                    'stock_name': String(50),
                    'stock_code': String(12),
                    'stock_target_price': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),'oracle'),
                    'report_title': String(100),
                    'report_opinion': String(50),
                    'report_pubdate': String(20),
                    'report_researcher': String(50),
                    'report_link': String(100)
                })
    return research