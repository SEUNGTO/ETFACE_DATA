import pdb
import pandas as pd
import requests
import zipfile
from xml.etree.ElementTree import parse
from io import BytesIO
import time
import os
from tqdm import tqdm

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
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFACE_DATA/refs/heads/main/data/dart_code_list.json'
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


if __name__ == '__main__' : 

    # dart_codeListing()
    dart_code_list = fetch_dart_code()
    corp_code_list = dart_code_list['고유번호'].to_list()

    YEAR = '2023'
    REPRT_CODE = '11011'

    data = pd.DataFrame({})
    error_list = []

    for CORP_CODE in tqdm(corp_code_list) :
        try : 

            tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'CFS')
            if tmp is not None :
                # 연결재무제표 데이터를 받은 경우
                tmp['fs_div'] = 'CFS'
                data = pd.concat([data, tmp])

            else : 
                tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'OFS')
                if tmp is not None :
                    # 개별재무제표 데이터를 받은 경우
                    tmp['fs_div'] = 'OFS'
                    data = pd.concat([data, tmp])

                else : 
                    # 연결/개별재무제표 데이터가 모두 없는 경우
                    error_list.append(f'{CORP_CODE}_EMPTY')

        except : 
            error_list.append(f'{CORP_CODE}_ERROR')
        
    else :
        data.reset_index(drop=True).to_excel('data/fs-account.xlsx', index = False)
        pd.DataFrame({'corp_code' : error_list}).to_excel('data/error_list.xlsx', index = False)
    