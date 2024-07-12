# Financial Statement Crawler
# 2024-07-12 현재는 사용불가
# ※ 정상적으로 데이터가 조회가 되지 않음
# 추후에 확인되는대로 다시 실행 및 작업

import FinanceDataReader as fdr
import requests
from tqdm import tqdm
import pandas as pd
from zipfile import ZipFile
from io import BytesIO
from xml.etree.ElementTree import parse
import time

def get_DART_code(dart_api) :
    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key' : dart_api}

    response = requests.get(url, params = params).content

    with ZipFile(BytesIO(response)) as zipfile :
        zipfile.extractall('corpCode')

    xmlTree = parse('corpCode/corpCode.xml')
    root =  xmlTree.getroot()
    raw_list =  root.findall('list')

    corp_list = []

    for i in range(0, len(raw_list)) :
        corp_code = raw_list[i].findtext('corp_code')
        corp_name = raw_list[i].findtext('corp_name')
        stock_code = raw_list[i].findtext('stock_code')

        corp_list.append([corp_code, corp_name, stock_code])

    return pd.DataFrame(corp_list, columns = ['고유번호', '정식명칭', '종목코드'])


def get_DART_FS(API_KEY, CORPS, YEAR, REPRT_CODE) :
    url = 'https://opendart.fss.or.kr/api/fnlttMultiAcnt.json'
    params = {
        'crtfc_key' : API_KEY,
        'corp_code' : CORPS,
        'bsns_year' : YEAR,
        'reprt_code' : REPRT_CODE
    }
    response = requests.get(url, params=params)
    response = response.json()
    data = pd.DataFrame(response['list'])

    return data



if __name__ == '__main__' :

    # DART 종목코드 리스팅
    krx_code_list = fdr.StockListing('KRX')
    DART_API_KEY = '7c267770aa9ed40ecd3eba679bc3af3e1cbd5569'
    code_list = get_DART_code(DART_API_KEY)
    code_list = code_list[code_list['종목코드'].isin(krx_code_list['Code'])]

    # DART FS 수집
    data = pd.DataFrame({})
    step = 50
    for i in tqdm(range(0, len(code_list), step)) :
        corp = code_list[i:i+step]['고유번호'].to_list()
        CORPS = ", ".join(corp)
        tmp = get_DART_FS(DART_API_KEY, CORPS, '2024', '11013')
        data = pd.concat([data, tmp])
        time.sleep(0.5)
