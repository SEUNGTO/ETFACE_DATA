import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def create_etf_base_table(engine):
    """
    ETF 최근 거래거래를 정리한 테이블
    """

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
    """
    ETF 포트폴리오 상세내역(리포트 포함)
    """

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



# +---------------------------+
# |                           |
# |   ETF 재무제표 데이터      |
# |                           |
# +---------------------------+
def update_etf_finance(engine) :
    portfolio_query = f"SELECT etf_code, stock_code, recent_quantity FROM etf_base_table"
    portfolio = pd.read_sql(portfolio_query, con = engine)

    etf_list = portfolio['etf_code'].unique()
    data = pd.DataFrame()

    for etf_code in etf_list :
        buffer = portfolio.loc[portfolio['etf_code'] == etf_code, :]

        stocks = "','".join(buffer['stock_code'].tolist())
        stocks = "'" + stocks + "'"
        query = f"SELECT * FROM fs_data WHERE stock_code in ({stocks})" # WHERE stock_code = "{stock_code}"'
        fs = pd.read_sql(query, con = engine)

        if not fs.empty :
            tmp = fs.set_index('stock_code').join(buffer.set_index('stock_code'))
            tmp['account_amount'] = tmp['recent_quantity'] * tmp['acmount_per_share']
            tmp = tmp.reset_index().groupby('acount_name').sum()[['account_amount']].reset_index()
            tmp.columns = ['acount_name','amount']
            tmp['etf_code'] = etf_code

            if not tmp.empty : 
                data = pd.concat([data, tmp], ignore_index=True)
        
    data.to_sql('etf_finance', con = engine, if_exists = 'replace',
                dtype = {
                    'acount_name': String(15),
                    'etf_code': String(12),
                    'amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                }, index = False)
    
    return data
    


def filter_bs_account(fs_data) :

    # 자산
    bs = fs_data['sj_div'] == 'BS'

    # 1. 자산
    # 1) 자산 총계
    asset = bs & (
        (fs_data['account_id'] == 'ifrs-full_Assets') |
        fs_data['account_id'].str.contains('-표준계정코드 미사용-') & fs_data['account_nm'].str.contains('자산총계')
        )
    if len(fs_data[asset]) > 1 :
        asset = bs & fs_data['account_id'].str.contains('ifrs-full_Assets')
    
    df1 = fs_data[asset].copy()
    if not df1.empty : df1.loc[:, 'account_nm_kor'] = '자산총계'

    # 2) 유동자산
    current_asset = bs & (fs_data['account_id'] == 'ifrs-full_CurrentAssets')
    df2 = fs_data[current_asset].copy()
    if not df2.empty : df2.loc[:, 'account_nm_kor'] = '유동자산'

    # 3) 비유동자산
    non_current_asset = bs & (fs_data['account_id'] == 'ifrs-full_NoncurrentAssets')
    df3 = fs_data[non_current_asset].copy()
    if not df3.empty : df3.loc[:, 'account_nm_kor'] = '비유동자산'


    # 4) 현금 및 현금성자산
    cash = bs & (fs_data['account_id'] == 'ifrs-full_CashAndCashEquivalents')
    df4 = fs_data[cash].copy()
    if not df4.empty : df4.loc[:, 'account_nm_kor'] = '현금'

    # 5) 매출채권
    rcvb_idx = bs & (
        fs_data['account_id'].str.contains('ifrs-full_TradeAndOtherCurrentReceivables') |
        fs_data['account_id'].str.contains('ifrs-full_TradeReceivables') |
        fs_data['account_id'].str.contains('ifrs-full_CurrentTradeReceivables') |
        fs_data['account_id'].str.contains('dart_ShortTermTradeReceivable') |
        fs_data['account_id'].str.contains('-표준계정코드 미사용-') & fs_data['account_nm'].str.contains('매출채권')
    )

    if len(fs_data[rcvb_idx]) > 1 :

        if 'dart_ShortTermTradeReceivable' in fs_data[rcvb_idx]['account_id'].tolist() :
            receivable = bs & (fs_data['account_id'] == 'dart_ShortTermTradeReceivable')
        elif 'ifrs-full_CurrentTradeReceivables' in fs_data[rcvb_idx]['account_id'].tolist() :
            receivable = bs & (fs_data['account_id'] == 'ifrs-full_CurrentTradeReceivables')
        elif 'ifrs-full_TradeReceivables' in fs_data[rcvb_idx]['account_id'].tolist() :
            receivable = bs & (fs_data['account_id'] == 'ifrs-full_TradeReceivables')
        elif 'ifrs-full_TradeAndOtherCurrentReceivables' in fs_data[rcvb_idx]['account_id'].tolist() :
            receivable = bs & (fs_data['account_id'] == 'ifrs-full_TradeAndOtherCurrentReceivables')
        else :
            idx = fs_data[rcvb_idx]['ord'].astype(int).idxmin()
            receivable = fs_data.index == idx

    else :
        receivable = rcvb_idx
    df5 = fs_data[receivable].copy()
    if not df5.empty : df5.loc[:, 'account_nm_kor'] = '매출채권'

    # 6) 재고자산
    inventory = bs & (
        fs_data['account_id'].str.contains('ifrs-full_Inventories') |
        fs_data['account_id'].str.contains('-표준계정코드 미사용-') & (fs_data['account_nm'] == '재고자산')
    )

    if len(fs_data[inventory]) > 1 :
        inventory = bs & fs_data['account_id'].str.contains('ifrs-full_Inventories')

    df6 = fs_data[inventory].copy()
    if not df6.empty : df6.loc[:, 'account_nm_kor'] = '재고자산'

    # 2. 자본
    # 1) 자기자본
    equity = bs & (fs_data['account_id'] == 'ifrs-full_Equity')
    df7 = fs_data[equity].copy()
    if not df7.empty : df7.loc[:, 'account_nm_kor'] = '자본총계'
    # 2) 이익잉여금
    retained_earning = bs & (fs_data['account_id'] == 'ifrs-full_RetainedEarnings')
    df8 = fs_data[retained_earning].copy()
    if not df8.empty : df8.loc[:, 'account_nm_kor'] = '이익잉여금'

    # 3. 부채
    # 1) 부채총계
    liability = bs & (fs_data['account_id'] == 'ifrs-full_Liabilities')
    df9 = fs_data[liability].copy()
    if not df9.empty : df9.loc[:, 'account_nm_kor'] = '부채총계'

    # 2) 유동부채
    current_liability = bs & (fs_data['account_id'] == 'ifrs-full_CurrentLiabilities')
    df10 = fs_data[current_liability].copy()
    if not df10.empty : df10.loc[:, 'account_nm_kor'] = '유동부채'

    # 3) 비유동부채
    non_current_liability = bs & (fs_data['account_id'] == 'ifrs-full_NoncurrentLiabilities')
    df11 = fs_data[non_current_liability].copy()
    if not df11.empty : df11.loc[:, 'account_nm_kor'] = '비유동부채'

    data = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])

    return data

def filter_pl_account(fs_data) :

    # 매출액
    revenue = fs_data['account_id'] == 'ifrs-full_Revenue'
    df1 = fs_data[revenue].copy()
    if 'CIS' in df1['sj_div'] :
        df1 = df1.loc[df1['sj_div'] == 'CIS', :]
    if not df1.empty : df1['account_nm_kor'] = '매출액'

    # 영업이익
    operating_income = (
        fs_data['account_id'].str.contains('ifrs-full_ProfitLossFromOperatingActivities') |
        fs_data['account_id'].str.contains('dart_OperatingIncomeLoss')
    )
    if len(fs_data[operating_income]) > 1 :
        operating_income = fs_data['account_id'].str.contains('ifrs-full_ProfitLossFromOperatingActivities')
    
    df2 = fs_data[operating_income].copy()
    if 'CIS' in df1['sj_div'] :
        df2 = df2.loc[df2['sj_div'] == 'CIS', :]
    if not df2.empty : df2['account_nm_kor'] = "영업이익"

    # 당기순이익
    net_income = (fs_data['sj_div'] == 'CIS') & (
        fs_data['account_id'] == 'ifrs-full_ProfitLoss'
        )
    df3 = fs_data[net_income].copy()
    if not df3.empty : df3['account_nm_kor'] = "당기순이익"

    data = pd.concat([df1, df2, df3])
    
    return data