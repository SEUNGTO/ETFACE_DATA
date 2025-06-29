#%%
import pdb
import re
import requests
import pandas as pd
from tqdm import tqdm
from modules_data.database import *
from dateutil.relativedelta import relativedelta
from config.config import *
from sqlalchemy import String

#%%
# 2. Dart 코드 불러오기
def read_code_table(engine) : 
    code_list = pd.read_sql("SELECT dart_code FROM code_table", con = engine)
    code_list = code_list.dropna()
    return code_list

# 3. Dart 기업정보 업데이트
def fetch_dart_company_info(CORP_CODE) :
    url = 'https://opendart.fss.or.kr/api/company.json' # 공시정보 > 기업개황
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : CORP_CODE,
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
    response = requests.get(url, params=params, headers=headers)
    buffer = response.json()

    if buffer['status'] == '000' :
        result = {
            'dart_code' : buffer.get('corp_code',''),       # 고유번호
            'stock_name' : buffer.get('stock_name', ''),    # 종목명
            'stock_code' : buffer.get('stock_code', ''),    # 종목코드
            'induty_code' : buffer.get('induty_code', ''), # 업종분류코드
            'est_dt' : buffer.get('est_dt', ''),    # 설립일
            'acc_mt' : buffer.get('acc_mt', '')     # 결산월
        }    
        return result

def update_dart_company_info(engine, corp_code_list) : 

    company_info = pd.read_sql('SELECT * FROM dart_company_info', con = engine)
    
    # (1) 기존 Dart 기업정보에는 없으나 업데이트한 코드테이블에는 없는 경우 >> 신규로 추가할 코드
    new_code = [c for c in corp_code_list['dart_code'] if c not in company_info['dart_code'].tolist()]

    if len(new_code) > 0 :   
        new_infos = []
        for CORP_CODE in new_code :
            buffer = fetch_dart_company_info(CORP_CODE)
            new_infos.append(buffer)
        new_infos = pd.DataFrame(new_infos)
        company_info = pd.concat([company_info, new_infos])
        
        # (2) 코드 테이블에만 있는 기업정보만 남김 (코드테이블에 없는 기업정보는 삭제)
        tt = corp_code_list['dart_code'].tolist()
        company_info[company_info['dart_code'].isin(tt)]
        company_info.to_sql('dart_company_info',con = engine, if_exists='replace', index = False)

    return company_info

# 4. 최신 보고서 정보 확인
def update_recent_report_list(now, freq, engine) :

    bgn_de = (now - relativedelta(days=freq)).strftime('%Y%m%d')
    
    result = []
    update_list = []

    page_no = 1

    while True : 

        url = 'https://opendart.fss.or.kr/api/list.json'
        params = {
            'crtfc_key': os.environ.get('DART_API_KEY'),
            'bgn_de' : bgn_de,
            'pblntf_ty' : 'A', # 정기공시
            'last_reprt_at' : 'Y', # 최종보고서 여부
            'page_no' : page_no,
            'page_count' : 100,
        }
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
            }
        response = requests.get(url, params = params, headers=headers)
        buffer = response.json()
        
        if buffer['status'] == '000' :

            item_list = buffer['list']

            for item in item_list :

                data = {
                    'dart_code' : item.get('corp_code', ''),
                    'report_nm' : re.sub("\[.*\]", "", item.get('report_nm', '')),
                    'rcept_no' : item.get('rcept_no', ''),
                    'rcept_dt' : item.get('rcept_dt', '')
                    }
                
                # 기존 저장된 보고서인지 확인
                check_query = f"""
                SELECT * 
                FROM dart_recent_report 
                WHERE dart_code = '{data['dart_code']}' 
                and report_nm = '{data['report_nm']}'
                """
                check = pd.read_sql(check_query, con = engine)

                if check.empty :    # 기존 테이블에 없는 경우(비어있는 경우) > 신규 데이터이므로 result에 추가 / 업데이트 대상에 추가
                    result.append(data)
                    update_list.append(data)
                    continue
                    
                elif check['rcept_no'] == data['rcept_no'] : 
                    # 기존 테이블에 있고, 보고서번호도 같은 경우 > 수정할 필요 없음
                    continue

                else : 
                    # 기존 테이블에 있지만, 보고서번호가 다른 경우 > 정정보고서이므로 rcept_no 수정 / 업데이트 대상에 추가
                    query = f"""
                    UPDATE dart_recent_report
                    SET rcept_np = {data['rcept_no']}
                    """
                    with engine.begin() as conn :
                        conn.execute(query)

                    update_list.append(data)
            
            if len(item_list) < 100 : # 마지막 페이지인 경우 Break
                break
            else :
                page_no += 1

    # 수집한 결과를 dart_recent_report에 업데이트
    pd.DataFrame(result).to_sql('dart_recent_report', 
                         con = engine, 
                         if_exists='append',
                         index = False,
                         dtype = {
                             'dart_code' : String(8),
                             'report_nm' : String(30),
                             'rcept_no' : String(14),
                             'rcept_dt' : String(8)
                         })

    return pd.DataFrame(update_list)
        

# 5. 업데이트 대상 데이터 수집
def fetch_update_data(CORP_CODE, YEAR, REPORT_CODE, FS_DIV) :
    url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
    params = {
        'crtfc_key' : os.getenv('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bsns_year' : YEAR, 
        'reprt_code' : REPORT_CODE, # 1분기보고서 : 11013, 반기보고서 : 11012, 3분기보고서 : 11014, 사업보고서 : 11011
        'fs_div' : FS_DIV, # OFS:재무제표, CFS:연결재무제표
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
    response = requests.get(url, params = params, headers=headers).json()

    if response['status'] == '000' :

        data = response['list']

        pdb.set_trace()


    pass

if __name__ == '__main__' :

    # # 1. DB 엔진 생성
    # engine = create_db_engine()

#%%

    # # 2. Dart 코드 불러오기
    # corp_code_list = read_code_table(engine)

    # # 3. Dart 기업정보 업데이트
    # company_info = update_dart_company_info(engine, corp_code_list)

    # # 4. 최신 보고서 정보 확인
    # update_list = update_recent_report_list(now, freq = 90, engine=engine)

    # 5. 업데이트 대상 데이터 수집 (2025. 6. 11.) // 작업환경이 좋지 않아 일단 초안만 작성
    """
    'dart_code' : String(8),
    'report_nm' : String(30),
    'rcept_no' : String(14),
    'rcept_dt' : String(8)
    """
    # fetch_update_data('00126380', '2024', '11011', 'OFS')

    # XBRL택사노미재무제표양식
    
    
    
    sj_div_list = ['BS1', 'BS2','BS3','BS4', 'CIS1', 'CIS2', 'CIS3', 'CIS4',
                   ]
    
    data = pd.DataFrame()
    url = 'https://opendart.fss.or.kr/api/xbrlTaxonomy.json'
    
    for sj_div in sj_div_list :
        params = {
            'crtfc_key' : os.getenv('DART_API_KEY'),
            'sj_div' : sj_div
            }
        response = requests.get(url, params=params).json()
        
        if response['status'] == '000' :
            buffer = pd.DataFrame(response['list'])
            data = pd.concat([data, buffer])

    data = data.rename(columns = {
        'sj_div' : '재무제표구분',
        'account_id' : '계정ID',
        'account_nm' : '계정명',
        'bsns_de' : '기준일',
        'label_kor' : '한글명',
        'label_eng' : '영문명',
        'data_tp' :'데이터 유형', 
        'ifrs_ref' :'IFRS Reference' 	# IFRS Reference ※ 출력예시 K-IFRS 1001 문단 54 (9),K-IFRS 1007 문단 45
        })
    data.sort_values(['한글명', '재무제표구분', '계정명', '계정ID']).to_csv('xbrl_code.csv', sep = "\t")
    #  ※ 데이타 유형설명 
    # - text block : 제목 
    # - Text : Text 
    # - yyyy-mm-dd : Date 
    # - X : Monetary Value 
    # - (X): Monetary Value(Negative) 
    # - X.XX : Decimalized Value 
    # - Shares : Number of shares (주식 수) 
    # - For each : 공시된 항목이 전후로 반복적으로 공시될 경우 사용 
    # - 공란 : 입력 필요 없음


    accounts = [
            '현금', '현금성자산', '현금 및 현금성자산', 
            '재고자산', '기타재고',
            '매출채권', '매출채권 및 기타유동채권',
            '매입채무', '매입채무 및 기타유동채무',
            '유동자산', '기타유동자산', 
            '유동부채', '기타유동부채', '유동성장기미지급금',
            '비유동자산', '기타비유동자산', '장기매출채권', '장기매출채권 및 기타비유동채권', 
            '비유동부채', '기타비유동부채', '장기매입채무', '장기매입채무 및 기타비유동채무'
            ]
    tmp = data[data['한글명'].isin(accounts)]
    tmp.sort_values(['한글명', '재무제표구분']).to_csv('계정명.csv', sep = "\t", index = False)
    # 유동자산, 비유동자산, 유동부채, 비유동부채는 유동/비유동법에서만 출현
    # 유동성배열법은 금융회사들이 주로 채택
    # 일단, 데이터를 다 받자.

    pdb.set_trace()


    """
    ### 단순합산하면 될지는 따져봐야 함
    ### 기업마다 매입채무만 보고하거나, 매입채무 및 기타유동채무만 보고하거나 한다면 깔끔하겠지만, 그렇지 않은 경우를 따져보아야 함
    ### 특히 유동자산/부채의 경우, 유동자산/부채가 현금, 재고자산 등의 총계를 말한다면 기타유동자산/부채는 하위계정일 가능성이 있음

    - 현금, 현금성자산, 현금 및 현금성자산 : ifrs_Cash, ifrs_CashEquivalents, ifrs_CashAndCashEquivalents
    - 재고자산, 기타재고 : ifrs_Inventories, dart_OtherInventoriesGross
    - 매출채권, 매출채권 및 기타유동채권 : dart_ShortTermTradeReceivable, ifrs_TradeAndOtherCurrentReceivables
    - 매입채무, 매입채무 및 기타유동채무 : 
    
    - 유동자산, 기타유동자산: ifrs_CurrentAssets, dart_OtherCurrentAssets
    - 유동부채, 기타유동부채, 유동성장기미지급금 : ifrs_CurrentLiabilities, dart_OtherCurrentLiabilities

    - 비유동자산, 기타비유동자산, 장기매출채권, 장기매출채권 및 기타비유동채권 : 
    - 비유동부채, 기타비유동부채, 장기매입채무, 장기매입채무 및 기타비유동채무 : 

    """

    """
    선행되어야 할 작업
    1) 표준코드미사용 기업의 비율은 얼마나 되는가
    2) 기업이 계정코드를 어떤 식으로 사용하고 있는가 > 예를 들어 매입채무와 매입채무 및 기타유동채무가 동시에 등장하는 경우는 없는가?
    3) 
    
    """
