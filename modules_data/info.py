import FinanceDataReader as fdr
import pandas as pd
from modules_data.krx import *
from modules_data.dart import *
from modules_data.database import *
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

import pdb

def update_basic_information(engine) :
    stock = update_stock_profile(engine)
    etf = update_etf_profile(engine)
    dart = fetch_dart_code()

    pdb.set_trace()
    
    # 1. 종목코드 리스트 업데이트
    update_code_list(engine)



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


def update_stock_profile(engine) : 

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

def update_etf_profile(engine) : 

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