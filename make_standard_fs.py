import pdb
import pandas as pd
from tqdm import tqdm


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
    
    df1 = fs_data[asset]

    # 2) 유동자산
    current_asset = bs & (fs_data['account_id'] == 'ifrs-full_CurrentAssets')
    df2 = fs_data[current_asset]

    # 3) 비유동자산
    non_current_asset = bs & (fs_data['account_id'] == 'ifrs-full_NoncurrentAssets')
    df3 = fs_data[non_current_asset]

    # 4) 현금 및 현금성자산
    cash = bs & (fs_data['account_id'] == 'ifrs-full_CashAndCashEquivalents')
    df4 = fs_data[cash]

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
    df5 = fs_data[receivable]

    # 6) 재고자산
    inventory = bs & (
        fs_data['account_id'].str.contains('ifrs-full_Inventories') |
        fs_data['account_id'].str.contains('-표준계정코드 미사용-') & (fs_data['account_nm'] == '재고자산')
    )

    if len(fs_data[inventory]) > 1 :
        inventory = bs & fs_data['account_id'].str.contains('ifrs-full_Inventories')

    df6 = fs_data[inventory]

    # 2. 자본
    # 1) 자기자본
    equity = bs & (fs_data['account_id'] == 'ifrs-full_Equity')
    df7 = fs_data[equity]
    # 2) 이익잉여금
    retained_earning = bs & (fs_data['account_id'] == 'ifrs-full_RetainedEarnings')
    df8 = fs_data[retained_earning]

    # 3. 부채
    # 1) 부채총계
    liability = bs & (fs_data['account_id'] == 'ifrs-full_Liabilities')
    df9 = fs_data[liability]
    # 2) 유동부채
    current_liability = bs & (fs_data['account_id'] == 'ifrs-full_CurrentLiabilities')
    df10 = fs_data[current_liability]
    # 3) 비유동부채
    non_current_liability = bs & (fs_data['account_id'] == 'ifrs-full_NoncurrentLiabilities')
    df11 = fs_data[non_current_liability]

    data = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])
    data['account_nm_kor'] = ['자산총계', '유동자산', '비유동자산', '현금', '매출채권', '재고자산', '자본총계', '이익잉여금', '부채총계', '유동부채', '비유동부채']

    return data

def filter_pl_account(fs_data) :

    # 매출액
    revenue = fs_data['account_id'] == 'ifrs-full_Revenue'
    df1 = fs_data[revenue]
    if 'CIS' in df1['sj_div'] :
        df1 = df1.loc[df1['sj_div'] == 'CIS', :]

    # 영업이익
    operating_income = (
        fs_data['account_id'].str.contains('ifrs-full_ProfitLossFromOperatingActivities') |
        fs_data['account_id'].str.contains('dart_OperatingIncomeLoss')
    )
    if len(fs_data[operating_income]) > 1 :
        operating_income = fs_data['account_id'].str.contains('ifrs-full_ProfitLossFromOperatingActivities')
    
    df2 = fs_data[operating_income]
    if 'CIS' in df1['sj_div'] :
        df2 = df2.loc[df2['sj_div'] == 'CIS', :]

    # 당기순이익
    net_income = (fs_data['sj_div'] == 'CIS') & (
        fs_data['account_id'] == 'ifrs-full_ProfitLoss'
        )
    df3 = fs_data[net_income]

    data = pd.concat([df1, df2, df3])
    data['account_nm_kor'] = ['매출액', '영업이익', '당기순이익']
    
    return data


if __name__ == "__main__" :


    # 전체 재무제표 계정 불러오기
    fs_all = pd.read_excel('data/fs-account.xlsx', dtype = str)

    # 대상 기업 정보 불러오기
    corp_list = pd.read_json('data/etf_fs_target_company.json', dtype = str)

    # 결과 데이터
    data = pd.DataFrame()

    for idx, v in tqdm(corp_list.iterrows()) :
        corp_code, stock_name, stock_code = v

        tmp = fs_all.loc[fs_all['corp_code'] == corp_code, :]
        balance_sheet = filter_bs_account(tmp)
        income_statement = filter_pl_account(tmp)
        tmp = pd.concat([balance_sheet, income_statement])

        data = pd.concat([data, tmp])
    else :
        data.reset_index(drop=True).to_excel('data/fs-account-filtered.xlsx', index = False)

    pdb.set_trace()
