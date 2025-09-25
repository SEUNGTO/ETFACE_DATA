import time
import FinanceDataReader as fdr
import pandas as pd
from modules_data.krx import *
from modules_data.dart import *
from modules_data.database import *
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def update_basic_information(engine) :
    stock = update_krx_stock_info(engine)
    etf = update_krx_etf_info(engine)
    dart = fetch_dart_code()

    stock = stock[['표준코드', '단축코드']]
    etf = etf[['표준코드', '단축코드']]
    krx = pd.concat([stock, etf])
    krx = krx.rename(columns = {'표준코드' : 'krx_code', '단축코드' : 'code'})
    
    dart = dart.drop('정식명칭', axis = 1)
    dart = dart.rename(columns = {'고유번호' : 'dart_code', '종목코드' : 'code'})

    data = krx.set_index('code').join(dart.set_index('code'), how = 'left')
    data.reset_index(inplace = True)
    
    # 1. 전체 코드 테이블 업데이트
    data.to_sql('code_table', con = engine, if_exists='replace')
    
    # 2. 종목 코드 업데이트
    update_code_list(engine)

    # 3. 회사 기본 정보 업데이트
    # [디버깅 중] 로컬 환경에서는 작동하지만 깃허브 액션에서 작동 안함. 
    # 5건 정도만 요청이 성공하고, 오류가 생기는데 이유를 모르겠음.
    # update_dart_company_info(engine)
    
    # 4. 상장주식정보 업데이트(KRX)
    update_all_stock_information(engine)
    

# +---------------------+
# | 한국거래소(KRX) 정보 |
# +---------------------+
def update_code_list(engine):

    stocks = fetch_krx_stock_code()
    stocks.loc[:, 'Type'] = 'Stock'

    krx_code = fetch_krx_etf_code()
    krx_code = krx_code['단축코드'].astype(str)
    etfs = fdr.StockListing('ETF/KR')
    etfs = etfs.loc[:, ['Name', 'Symbol']]
    etfs.loc[:, 'Type'] = 'ETF'
    etfs = etfs[etfs['Symbol'].isin(krx_code)]

    code_list = pd.concat([stocks, etfs])
    code_list.reset_index(drop=True)

    # DB에 반영
    code_list.to_sql('code_list', engine, if_exists='replace', index=False)

    return code_list

def update_krx_stock_info(engine) : 

    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01901'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
    otp = requests.post(otp_url, params=otp_params, headers=headers).text
    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code': otp}
    response = requests.post(down_url, params=down_params, headers=headers)
    data = pd.read_csv(io.BytesIO(response.content), encoding='euc-kr', dtype={'단축코드': 'string'})

    data.to_sql('stock_info', con = engine, if_exists='replace')

    return data

def update_krx_etf_info(engine) : 
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'share': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT04601'
    }
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}

    otp = requests.post(otp_url, params=otp_params, headers=headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code': otp}
    response = requests.post(down_url, params=down_params, headers=headers)

    data = pd.read_csv(io.BytesIO(response.content), encoding='euc-kr', dtype={'단축코드': 'string'})
    
    data.drop(['한글종목명', '영문종목명'], axis = 1, inplace = True)
    data['상장일'] = data['상장일'].str.replace("/", "-")
    data.to_sql('etf_info', con = engine, if_exists='replace',
                dtype={
                    '표준코드': String(12),
                    '단축코드': String(6),
                    '한글종목약명': String(35),
                    '상장일' : String(10),
                    '기초지수명' : String(100),
                    '지수산출기관' : String(50),
                    '추적배수' : String(10),
                    '복제방법' : String(10),
                    '기초시장분류': String(5),
                    '기초자산분류' : String(5),
                    '상장좌수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '운용사' : String(15),
                    'CU수량' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '총보수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '과세유형' : String(20)
                    })

    return data


# +----------------------+
# | 금융감독원(DART) 정보 |
# +----------------------+
def update_dart_company_info(engine) :
    dart_code_list = read_dart_code(engine)

    buffer = []
    for dart_code in dart_code_list :
        print(dart_code)
        item = fetch_dart_company_info(dart_code)
        buffer.append(item)
        time.sleep(0.5)
    data = pd.DataFrame(buffer)

    data.drop(['status', 'message'], axis = 1, inplace = True)
    data.rename(columns = {
        'corp_code':'고유번호',
        'corp_name':'정식명칭',
        'corp_name_eng':'영문명칭',
        'stock_name':'종목명',
        'stock_code':'종목코드',
        'ceo_nm':'대표자명',
        'corp_cls':'법인구분',
        'jurir_no':'법인등록번호',
        'bizr_no':'사업자등록번호',
        'adres':'주소',
        'hm_url':'홈페이지',
        'ir_url':'IR홈페이지',
        'phn_no':'전화번호',
        'fax_no':'팩스번호',
        'induty_code':'업종코드',
        'est_dt':'설립일',
        'acc_mt':'결산월',
        }, inplace = True)
    
    data.to_sql('company_info', con = engine, if_exists='replace')

    return data

def read_dart_code(engine) : 
    code_list = pd.read_sql("SELECT * FROM code_table", con = engine)
    code_list = code_list['dart_code'].dropna()
    return code_list

def fetch_dart_company_info(dart_code) :
    url = 'https://opendart.fss.or.kr/api/company.json'
    params = {'crtfc_key': os.environ.get('DART_API_KEY'),
              'corp_code' : dart_code}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
    response = requests.get(url, params = params, headers = headers)
    data = response.json()

    if data['status'] == '000' :
        return data
    else : 
        raise AssertionError
