import FinanceDataReader as fdr
import pandas as pd
import requests
import io
from data_modules.krx import *


def update_code_list(engine):

    stocks = fetch_krx_stock_code()
    stocks.loc[:, 'Type'] = 'Stock'

    krx_code = fetch_krx_code()
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

def fetch_krx_stock_code():

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
    data = data[['한글 종목약명', '단축코드']]
    data.columns = ['Name', 'Symbol']

    return data


def update_profile(engine) :
    """
    ETF/Stock의 주요 프로필을 업데이트
    넣고자 하는 기능 
     - ETF : KRX에서 제공하는 기본 데이터, 최근 얼마나 주목받는지, 수급은 얼마나 되는지, 보유종목의 특징은 무엇인지, 손바뀜은 얼마나 잦은지
     - Stock : KRX 등에서 제공하는 기본 데이터, 최근 얼마나 주목받는지, 수급은 얼마나 되는지, 보유종목의 특징은 무엇인지, 손바뀜은 얼마나 잦은지
    
    + 종토넷 등의 커뮤니티에서 주목받는 정도...?    
    """
    pass