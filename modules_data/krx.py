import io
import time
import requests
import numpy as np
import pandas as pd
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT


def update_krx_etf_data(date, table_name, engine):

    codelist = fetch_krx_etf_code()
    
    for i, (isuCd, code, name) in enumerate(zip(codelist['표준코드'], codelist['단축코드'], codelist['한글종목약명'])):

        if i == 0:
            data = fetch_portfolio(isuCd, code, name, date)
            data.insert(0, 'ETF코드', code)
            data = data.drop(['시가총액', '시가총액 구성비중'], axis=1)
            data.loc[:, '비중'] = data['평가금액'] / data['평가금액'].sum() * 100
            time.sleep(np.random.rand())

        else:
            tmp = fetch_portfolio(isuCd, code, name, date)
            tmp.insert(0, 'ETF코드', code)
            tmp = tmp.drop(['시가총액', '시가총액 구성비중'], axis=1)
            tmp.loc[:, '비중'] = tmp['평가금액'] / tmp['평가금액'].sum() * 100
            data = pd.concat([data, tmp])
            time.sleep(np.random.rand())

    data.columns = ['etf_code', 'stock_code', 'stock_nm', 'stock_amn', 'evl_amt', 'ratio']
    data = data.reset_index(drop=True)

    data.to_sql(table_name, engine, if_exists='replace', index=False,
                dtype={
                    'etf_code': String(12),
                    'stock_code': String(12),
                    'stock_nm': String(50),
                    'stock_amn': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'evl_amt': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    'ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle')
                })

    return data

def fetch_krx_etf_code():

    # ETFACE v1.0 : 국내 주식시장만 취급
    # KRX에서 크롤링할 때만 사용함
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
    _filter = (data['기초시장분류'] == '국내') & (data['기초자산분류'] == '주식')
    data = data[_filter]

    return data

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




def fetch_portfolio(isuCd, code, name, date):
    headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
    otp_url = 'http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd'
    otp_params = {
        'locale': 'ko_KR',
        'tboxisuCd_finder_secuprodisu1_0': f'{code}/{name}',
        'isuCd': f'{isuCd}',
        'isuCd2': f'{isuCd}',
        'codeNmisuCd_finder_secuprodisu1_0': f'{name}',
        'param1isuCd_finder_secuprodisu1_0': "",
        'trdDd': f'{date}',
        'share': '1',
        'money': '1',
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT05001'
    }

    otp = requests.post(otp_url, params=otp_params, headers=headers).text

    down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
    down_params = {'code': otp}
    response = requests.post(down_url, params=down_params, headers=headers)

    data = pd.read_csv(io.BytesIO(response.content),
                        encoding='euc-kr',
                        dtype={'단축코드': str})

    return data

