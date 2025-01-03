import pandas as pd
import requests
import zipfile
from xml.etree.ElementTree import parse
from io import BytesIO
import os

def dart_codeListing():
    """
    매일 돌아가면서 DART에서 코드를 리스팅할 함수
    """

    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': os.environ.get('DART_API_KEY')}

    response = requests.get(url, params=params).content

    with zipfile.ZipFile(BytesIO(response)) as z:
        z.extractall('corpCode')

    xmlTree = parse(os.path.join(os.getcwd(), 'corpCode/CORPCODE.xml'))
    root = xmlTree.getroot()
    raw_list = root.findall('list')

    corp_list = []

    for i in range(0, len(raw_list)):
        corp_code = raw_list[i].findtext('corp_code')
        corp_name = raw_list[i].findtext('corp_name')
        stock_code = raw_list[i].findtext('stock_code')

        corp_list.append([corp_code, corp_name, stock_code])


    if not os.path.exists('data') :
        os.makedirs('data')
    
    dart_code_list = pd.DataFrame(corp_list, columns=['고유번호', '정식명칭', '종목코드'])
    dart_code_list = dart_code_list.loc[dart_code_list['종목코드'] != " ", :]
    dart_code_list = dart_code_list.reset_index(drop = True)

    # JSON 파일로 저장
    dart_code_list.to_json('data/dart_code_list.json')


def fetch_dart_code() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFACE_DATA/refs/heads/main/data/etf_fs_target_company.json'
    dart_code_list = requests.get(url).json()
    dart_code_list = pd.DataFrame(dart_code_list)

    return dart_code_list


def fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, FS_DIV) :

    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'

    params = {
        'crtfc_key': os.environ.get('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bsns_year' : YEAR,
        'reprt_code' : REPRT_CODE,
        'fs_div' : FS_DIV,
    }

    response = requests.get(url, params = params).json()

    if response['status'] == '000' : 
        return pd.DataFrame(response['list'])

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




if __name__ == '__main__' : 

    # dart_codeListing()
    dart_code_list = fetch_dart_code()
    corp_code_list = dart_code_list['고유번호'].to_list()

    YEAR = '2023'
    REPRT_CODE = '11011'

    data = pd.DataFrame({})
    error_list = []

    for CORP_CODE in corp_code_list :
        try : 

            tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'CFS')
            if tmp is not None :
                # 연결재무제표 데이터를 받은 경우
                tmp['fs_div'] = 'CFS'
                bs = filter_bs_account(tmp)
                pl = filter_pl_account(tmp)
                tmp = pd.concat([bs, pl])
                data = pd.concat([data, tmp])

            else : 
                tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'OFS')
                if tmp is not None :
                    # 개별재무제표 데이터를 받은 경우
                    tmp['fs_div'] = 'OFS'
                    bs = filter_bs_account(tmp)
                    pl = filter_pl_account(tmp)
                    tmp = pd.concat([bs, pl])
                    data = pd.concat([data, tmp])

                else : 
                    # 연결/개별재무제표 데이터가 모두 없는 경우
                    error_list.append(f'{CORP_CODE}_EMPTY')

        except : 
            error_list.append(f'{CORP_CODE}_ERROR')
        
    else :
        data.reset_index(drop=True).to_excel('data/fs-account.xlsx', index = False)
        pd.DataFrame({'corp_code' : error_list}).to_excel('data/error_list.xlsx', index = False)
    