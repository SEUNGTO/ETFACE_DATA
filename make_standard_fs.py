import pdb
import pandas as pd


def filter_account(fs_data) :
    
    bs = fs_data['sj_div'] == 'BS'

    # 자산 파트
    # 1. 자산 총계    
    asset = bs & (fs_data['account_id'] == 'ifrs-full_Assets')
    asset = fs_data[asset]

    # 유동자산
    current_asset = bs & (fs_data['account_id'] == 'ifrs-full_CurrentAssets')
    current_asset = fs_data[current_asset]

    # 비유동자산
    non_current_asset = bs & (fs_data['account_id'] == 'ifrs-full_CurrentAssets')
    non_current_asset = fs_data[non_current_asset]

    # 현금 및 현금성자산
    cash = bs & (fs_data['account_id'] == 'ifrs-full_CashAndCashEquivalents')
    cash = fs_data[cash]

    # 매출채권
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
        elif 'ifrs-full_TradeAndOtherCurrentReceivables' in fs_data[rcvb_idx]['account_id'].tolist() :
            receivable = bs & (fs_data['account_id'] == 'ifrs-full_TradeAndOtherCurrentReceivables')
        else :
            receivable = fs_data[rcvb_idx]['ord'].idxmin()

        receivable = fs_data[receivable]

    else :
        receivable = fs_data[rcvb_idx]
    
    # 재고자산
    inventory = bs & (
        fs_data['account_id'].str.contains('ifrs-full_Inventories') |
        fs_data['account_id'].str.contains('-표준계정코드 미사용-') & (fs_data['account_nm'] == '재고자산')
    )

    if len(fs_data[inventory]) > 1 :
        inventory = bs & fs_data['account_id'].str.contains('ifrs-full_Inventories')
    
    inventory = fs_data[inventory]

    asset_side = pd.concat([asset, current_asset,non_current_asset, cash, receivable, inventory])
    asset_side['account_nm_kor'] = ['자산총계', '유동자산', '비유동자산', '현금', '매출채권', '재고자산']

    pdb.set_trace()


if __name__ == "__main__" :

    fs_all = pd.read_excel('data/fs-account.xlsx')
    corp_list = pd.read_excel('data/etf_fs_target_company.xlsx', dtype = str)

    for idx, v in corp_list.iterrows() :
        corp_code, stock_name, stock_code = v

        tmp = fs_all.loc[fs_all['corp_code'] == corp_code, :]
        
        filter_account(tmp)

        


