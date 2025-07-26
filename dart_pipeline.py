# 2025. 6. 25.
"""
기본적인 데이터 수집/추출 로직은 완료함.
다만 디테일하게 다듬어야 할 부분들이 있다.
1) 일단 적재하기 좋은 데이터로 바꾸어야 놓아야 한다.
 - 전체 데이터와 전체 재무제표 데이터가 공유하는 Column에 차이가 있으니, 일차적으로 클리닝 작업을 하고
 - 최종적으로 1주당 계정금액으로 쌓아놓아야 하니 변환해두어야 한다

이후에 필요한 작업은 다음과 같다
1) 주기적으로 배치파일을 돌리고
2) 업데이트 해야 하는지 판단한 후에
3) 업데이트 해야 할 사항은 데이터를 업데이트 하고, 
4) 업데이트가 완료되었음을 적어두어야 한다.


=> 이걸 모듈화로 하려면,
현재는 빠른 작업을 위해 다중기업의 재무제표를 불러왔지만,
1개 기업에 대해 하는 모듈로 바꾸어야 할 것으로 보임!

"""
#%%
import re
import os
import time
import pandas as pd
import requests
from tqdm import tqdm
from config.config import *
from modules_data.database import *
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def prevent_ban(fn) : 
    def wait(*args, **kwargs) :
        a = time.time()
        result = fn(*args, **kwargs)
        b = time.time() - a
        time.sleep(max(60/1000 - b, 0))

        return result

    return wait

@prevent_ban
def fetch_multi_fs_main_account(CORP_CODE, YEAR, REPRT_CODE) :
    url = 'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bsns_year' : YEAR,
        'reprt_code' : REPRT_CODE,
    }
    response = requests.get(url, params = params).json()
    
    if response['status'] == '000' : 
        data = response['list']
        return pd.DataFrame(data)
    
    else :
        return pd.DataFrame()

@prevent_ban    
def fetch_single_fs_all_account(CORP_CODE, YEAR, REPRT_CODE, FS_DIV) :
    url = 'https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bsns_year' : YEAR,
        'reprt_code' : REPRT_CODE,
        'fs_div' : FS_DIV, # 'CFS', 'OFS'
    }
    response = requests.get(url, params = params).json()
    
    if response['status'] == '000' :
        buffer = response['list']
        return pd.DataFrame(buffer)
    
    else :
        return pd.DataFrame()

@prevent_ban
def fetch_number_of_stocks(CORP_CODE, YEAR, REPRT_CODE) :
    time.sleep(60/1000)

    url = 'https://opendart.fss.or.kr/api/stockTotqySttus.json'
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bsns_year' : YEAR,
        'reprt_code' : REPRT_CODE,
    }
    
    response = requests.get(url, params = params).json()
    
    if response['status'] == '000' :

        stocks = pd.DataFrame(response['list'])
        stock = stocks.loc[stocks['se'] == '합계', 'distb_stock_co']
        stock = stock.values[0]
        
        if type(stock) == str :
            stock = re.sub("\D", "", stock)
            
        if len(stock) > 0 :
            return float(stock)

        else :
            # 분기에 보고할 의무가 없는 경우 : 전년도 보고서에서 가져오기
            params['bsns_year'] = str(int(params['bsns_year']) - 1)
            params['reprt_code'] = '11011'
            response = requests.get(url, params = params).json()
            if response['status'] == '000' :
                stocks = pd.DataFrame(response['list'])
                stock = stocks.loc[stocks['se'] == '합계', 'distb_stock_co']
                stock = stock.values[0]
                if type(stock) == str :
                    stock = re.sub("\D", "", stock)
                    if len(stock) > 0 :
                        return float(stock)
                    else : 
                        return None
    else :
        return None

def extract_terminal_cash(detail_data) :
    # +-----------------------------+
    # |                             |
    # |   1. 현금 및 현금성자산       |  
    # |                             |
    # +-----------------------------+

    # 현금흐름표 추출
    cash_statement = detail_data[detail_data['sj_div'] == 'CF'] ## 현금흐름표

    # 다음 키워드가 포함된 계정명은 포함
    inc_con = cash_statement['account_nm'].str.contains('현금')

    # 다음 키워드가 포함된 계정명은 제외 
    exclude_keyword = [
        '비현금', '외화', '활동',
        '흐름', '기초', '따른', 
        '영업', '투자', 
        '감소', '증가', '증감', '유출', '유입','변동', '가감',
        '예정', '매각', '배당', '이자', '환율', '지배력', '주식',
        '리스부채', '리스', '합병', '차입', '보조금', '종속',
        '환산', '환산', '결합', '매수', '처분',
    ]
    exclude_keywords = "|".join(exclude_keyword)
    exc_con = ~cash_statement['account_nm'].str.contains(exclude_keywords)
    cash = cash_statement[inc_con & exc_con]

    # 결과가 여럿인 경우 가장 위의 데이터만 가져옴
    filter_index = cash.groupby('corp_code')['ord'].idxmin()
    cash = cash.loc[filter_index]
    cash['account_nm'] = "현금"
    
    cash_out_sample = cash_statement[~cash_statement['corp_code'].isin(cash['corp_code'])]
    
    return cash, cash_out_sample


def extract_inventory(detail_data) : 
    # +-----------------------------+
    # |                             |
    # |   2. 재고자산                |  
    # |                             |
    # +-----------------------------+
    balance_sheet = detail_data[detail_data['sj_div'] == 'BS']
    inventory = balance_sheet[balance_sheet['account_nm'].str.contains('재고')]
    inventory.loc[:, 'account_nm'] = inventory['account_nm'].apply(lambda x : re.sub(r"\(.*\)", "", x))
    inventory.loc[:, 'account_nm'] = inventory['account_nm'].apply(lambda x : re.sub(r"[^ㄱ-ㅎ가-힇]", "", x)) # 숫자 제거
    inventory.loc[:, 'account_nm'] = inventory['account_nm'].str.replace("총유동재고자산", "재고자산")

    con1 = inventory['account_nm'] == '재고자산'
    con2 = inventory['account_nm'] == '유동재고자산'
    inventory = inventory[con1 | con2]
    inventory['account_nm'] = '재고자산'

    # 여기에 포함되지 않는 경우, 실제로 재고자산을 보고하지 않는 것으로 보임
    in_sample_index = inventory['corp_code']
    out_sample_bs = balance_sheet[~balance_sheet['corp_code'].isin(in_sample_index)]
    inventory_out_sample = out_sample_bs[out_sample_bs['account_id'].str.contains('Inven')]

    return inventory, inventory_out_sample


def extract_receivable(detail_data) : 
    # +-----------------------------+
    # |                             |
    # |   3. 매출채권                |  
    # |                             |
    # +-----------------------------+
    balance_sheet = detail_data[detail_data['sj_div'] == 'BS']
    receivable = balance_sheet[balance_sheet['account_nm'].str.contains('매출채권')]

    # 기초적인 전처리
    receivable.loc[:, 'account_nm'] = receivable['account_nm'].apply(lambda x : re.sub("[^ㄱ-ㅎ가-힇]", "", x))
    exclude_keyword = [
        '장기', '리스', '상각', '외의', '외',
        '미청구', '초과', '대출', '누계액',
        '대손', '현재가치', '제외', 
        '장기', '비유동', '비',
        ]
    exclude_keywords = "|".join(exclude_keyword)
    exc_con = ~receivable['account_nm'].str.contains(exclude_keywords)
    receivable = receivable[exc_con]

    exclude_id = [
        'Noncurrent', 'Long',
    ]
    exclude_ids = "|".join(exclude_id)
    receivable = receivable[~receivable['account_id'].str.contains(exclude_ids)]

    # 계정별로 하나만 있는 경우
    cnt = receivable['corp_code'].value_counts()
    one = cnt[cnt == 1].index.to_list()
    rcvb1 = receivable[receivable['corp_code'].isin(one)]

    # 계정별로 2개 이상 있는 경우
    many = cnt[cnt>1].index.to_list()
    rcvb2 = receivable[receivable['corp_code'].isin(many)]

    # 계정명 중 매출채권/단기매출채권이 있는 경우 >> 이걸로 뽑기
    con1 = rcvb2['account_nm'] == '매출채권'
    con2 = rcvb2['account_nm'] == '단기매출채권'
    rcvb2_1 = rcvb2[con1 | con2]
    idx = rcvb2_1.groupby('corp_code')['ord'].idxmin()
    rcvb2_1 = rcvb2_1.loc[idx]

    # 계정명이 모두 매출채권 및 기타OO채권인 경우 > 먼저 등장한 계정 가져오기
    rcvb2_2 = rcvb2[~rcvb2['corp_code'].isin(rcvb2_1['corp_code'])]
    idx = rcvb2_2.groupby('corp_code')['ord'].idxmin()
    rcvb2_2 = rcvb2_2.loc[idx]

    # 모두 합치기
    receivable = pd.concat([rcvb1, rcvb2_1, rcvb2_2])
    receivable['account_nm'] = '매출채권'

    # 여기에 포함되지 않는 경우, 실제로 매출채권을 보고하지 않는 것으로 보임
    in_sample_index = receivable['corp_code']
    out_sample_bs = balance_sheet[~balance_sheet['corp_code'].isin(in_sample_index)]
    out_receivable = out_sample_bs[out_sample_bs['account_id'].str.contains('Recei')]

    return receivable, out_receivable

def extract_payable(detail_data) : 
    # +-----------------------------+
    # |                             |
    # |   4. 매입채무                |  
    # |                             |
    # +-----------------------------+
    balance_sheet = detail_data[detail_data['sj_div'] == 'BS']
    payable = balance_sheet[balance_sheet['account_nm'].str.contains('매입채무')]
    payable.loc[:, 'account_nm'] = payable['account_nm'].apply(lambda x : re.sub("[^ㄱ-ㅎ가-힇]", "", x))

    # 제외할 키워드
    exclude_keyword = [
        '장기', '비유동', '비', '외',
        ]
    exclude_keywords = "|".join(exclude_keyword)
    exc_con = ~payable['account_nm'].str.contains(exclude_keywords)
    payable = payable[exc_con]

    # 계정명에서 삭제
    exclude_id = [
        'Noncurrent', 'Long',
    ]
    exclude_ids = "|".join(exclude_id)
    payable = payable[~payable['account_id'].str.contains(exclude_ids)]

    # 1개인 경우
    cnt = payable['corp_code'].value_counts()
    one = cnt[cnt == 1]
    payable1 = payable[payable['corp_code'].isin(one.index)]

    # 2개 이상인 경우
    # 매입채무와 단기매입채무가 있는 경우 그대로 가져옴
    many = cnt[cnt > 1]
    payable2 = payable[payable['corp_code'].isin(many.index)]
    con1 = payable2['account_nm'] == '매입채무'
    con2 = payable2['account_nm'] == '단기매입채무'
    payable2_1 = payable2[con1 | con2]

    idx = payable2_1.groupby('corp_code')['ord'].idxmin()
    payable2_1 = payable2_1.loc[idx] # 다만, 매입채무와 단기채무가 모두 있는 경우 먼저 보고된 계정을 가져옴

    # 그 외에는 먼저 report된 경우를 가져옴
    payable2_2 = payable2[~payable2['corp_code'].isin(payable2_1['corp_code'])]
    idx = payable2_2.groupby('corp_code')['ord'].idxmin()
    payable2_2 = payable2_2.loc[idx]

    payable = pd.concat([payable1, payable2_1, payable2_2])
    payable['account_nm'] = '매입채무'

    in_sample_index = payable['corp_code']
    out_sample_bs = balance_sheet[~balance_sheet['corp_code'].isin(in_sample_index)]
    out_payable = out_sample_bs[out_sample_bs['account_id'].str.contains('Paya')]
    
    return payable, out_payable


if __name__ == '__main__' :

    engine = create_db_engine()
    code_table = pd.read_sql('SELECT * FROM code_table', con = engine)
    code_table = code_table.set_index('dart_code')
    dart_code_list = pd.Series(code_table.index).dropna()
    dart_code_list = dart_code_list.tolist()
 
    # +-----------------------------+
    # |                             |
    # |      연도말 사업보고서        |  
    # |                             |
    # +-----------------------------+
    # [BS] 유동자산, 비유동자산, 자산총계
    # [BS] 유동부채, 비유동부채, 부채총계
    # [BS] 자본금, 이익잉여금, 자본총계
    # [IS] 매출액, 영업이익, 법인세차감전순이익, 당기순이익, 총포괄손익
    
    # 1분기보고서 : 11013
    # 반기보고서 : 11012
    # 3분기보고서 : 11014
    # 사업보고서 : 11011

    REPORT_CODE = '11013'
    REPORT_DATE = '2025-03-31'
    YEAR = '2025'

    print(f"보고서코드 : {REPORT_CODE} / 기준일자 : {REPORT_DATE}...")    

    print(f"1. 다중재무제표 수집 중...")
    interval = 5
    data = pd.DataFrame()
    for i in range(int(len(dart_code_list)/interval)) :
        codes = dart_code_list[i*interval:(i+1)*interval]
        code = ",".join(codes)
        buffer = fetch_multi_fs_main_account(code, YEAR, REPORT_CODE)
        data = pd.concat([data, buffer])
        
        print(f'Step : {i:04d} | data : {data.shape} | buffer : {buffer.shape}', end = "\r")
        
        # rcept_no가 있으니, 업데이트가 가능함

    # 연결재무제표, 개별재무제표가 모두 있다면 > 연결재무제표만 선택
    print()
    print(f"2. 연결/개별재무제표 선별 중...")
    df = pd.DataFrame()
    for corp_code in data['corp_code'].drop_duplicates() :
        buffer = data[data['corp_code'] == corp_code]
        if 'CFS' in buffer['fs_div'].to_list() :
            buffer = buffer[buffer['fs_div'] == 'CFS']
        df = pd.concat([df, buffer])

    
    # 매출채권, 재고자산, 현금
    print(f"3. 전체재무제표에서 매출채권, 재고자산, 현금, 매입채무 수집 중...")
    corp_code_list = data['corp_code'].drop_duplicates().to_list()
    detail_data = pd.DataFrame()

    for CORP_CODE in tqdm(corp_code_list, desc = "Single Detail") :
        buffer = fetch_single_fs_all_account(CORP_CODE, YEAR, REPORT_CODE, 'CFS')

        if buffer.empty :
            buffer = fetch_single_fs_all_account(CORP_CODE, YEAR, REPORT_CODE, 'OFS')
 
        detail_data = pd.concat([detail_data, buffer])
        
    detail_data = detail_data.reset_index(drop= True)
    cash, cash_out_sample = extract_terminal_cash(detail_data)
    inventory, inventory_out_sample = extract_inventory(detail_data)
    receivable, out_receivable = extract_receivable(detail_data)
    payable, out_payable = extract_payable(detail_data)
    
    
    ## 유통주식수
    print(f"4. 유통주식수 수집 중...")
    stocks = []
    for CORP_CODE in tqdm(data['corp_code'].drop_duplicates()) :
        stock = fetch_number_of_stocks(CORP_CODE, YEAR, REPORT_CODE)
        if stock : 
            buffer = {
                'corp_code' : CORP_CODE,
                'stocks' : stock
                }
            stocks.append(buffer)

    stocks = pd.DataFrame(stocks)
    
    print(f"5. 데이터 최종 전처리 중...")
    ## 개별재무제표에서 얻은 데이터들 정비
    add_data = pd.concat([cash, inventory, receivable, payable])
    cols = ['bsns_year', 'corp_code', 'account_nm', 'thstrm_amount']
    add_data_final = add_data[cols]
    add_data_final = add_data_final.set_index('corp_code').join(code_table['code']).reset_index()

    renamed_columns = {
        'bsns_year' : 'year',
        'corp_code' : 'corp_code',
        'code' : 'stock_code',
        'account_nm' : 'account_name',
        'thstrm_amount' : 'amount'
    }
    add_data_final = add_data_final.rename(columns = renamed_columns)
    add_data_final = add_data_final.set_index('corp_code').join(stocks.set_index('corp_code')).reset_index()
    add_data_final = add_data_final.dropna()
    add_data_final['amount'] = [0 if len(v) == 0 else float(v) for v in add_data_final['amount']]
    add_data_final['report_date'] = REPORT_DATE
    add_data_final['amount_per_share'] = add_data_final['amount'] / add_data_final['stocks']

    cols = ['year', 'report_date', 'stock_code', 'account_name', 'amount', 'stocks', 'amount_per_share']
    add_data_final = add_data_final[cols]
    
    ## 다중재무제표로부터 얻은 데이터들 정비
    cols = ['bsns_year', 'corp_code', 'stock_code', 'account_nm', 'thstrm_amount']
    final_data = df[cols]
    renamed_columns = {
        'bsns_year' : 'year',
        'corp_code' : 'corp_code',
        'stock_code' : 'stock_code',
        'account_nm' : 'account_name',
        'thstrm_amount' : 'amount',
    }
    final_data = final_data.rename(columns = renamed_columns)
    final_data['amount'] = final_data['amount'].str.replace(",", "")
    final_data['amount'] = [0 if v == '-' else float(v) for v in final_data['amount']]
    final_data['report_date'] = REPORT_DATE
    final_data = final_data.set_index('corp_code').join(stocks.set_index('corp_code')).reset_index()
    final_data = final_data.dropna()
    final_data['amount_per_share'] = final_data['amount'] / final_data['stocks']
    final_data = final_data.drop('corp_code', axis = 1)
    cols = ['year', 'report_date', 'stock_code', 'account_name', 'amount', 'stocks', 'amount_per_share']
    final_data = final_data[cols]
    final_data['year'] = final_data['year'].astype(str)

    ## 데이터 최종 병합 및 전처리
    final_data = pd.concat([final_data, add_data_final])
    final_data = final_data.drop_duplicates()

    final_data['year'] = final_data['year'].astype(str)
    final_data['report_date'] = final_data['report_date'].astype(str)
    final_data['stock_code'] = final_data['stock_code'].astype(str)
    final_data['account_name'] = final_data['account_name'].astype(str)
    final_data['amount_per_share'] = round(final_data['amount_per_share'], 4)
    
    final_data.to_csv(f'{REPORT_DATE}_{REPORT_CODE}.csv', sep = '\t')
