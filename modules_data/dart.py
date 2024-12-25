import re
import os
import time
import zipfile
import pandas as pd
import requests
from io import BytesIO
from xml.etree.ElementTree import parse
from dateutil.relativedelta import relativedelta
from config.config import *

def update_finance_base_table(engine) :

    code_list = read_dart_code(engine)
    data = pd.DataFrame()
    tmp = pd.DataFrame()

    for idx, CORP_CODE in enumerate(code_list) :
        time.sleep(0.7)

        print(idx, CORP_CODE, end = " ")
        try : 
            YEAR, REPRT_CODE = get_recent_report(CORP_CODE, now)

            if YEAR is not None : 
                time.sleep(0.7)
                tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'CFS')
                if not tmp.empty :
                    tmp['fs_div'] = 'CFS'
                    data = pd.concat([data, tmp])
                else :
                    tmp = fetch_finance_account(CORP_CODE, YEAR, REPRT_CODE, 'OFS')
                    if not tmp.empty :
                        # 개별재무제표 데이터를 받은 경우
                        tmp['fs_div'] = 'OFS'
                        data = pd.concat([data, tmp])
        except :
            continue
        finally :
            print(f"DATA : {data.shape}, TMP : {tmp.shape}")

    data.to_sql('finance_base', con = engine, if_exists='replace', index = False)
    
    return data

def read_dart_code(engine) : 
    code_list = pd.read_sql("SELECT * FROM code_table", con = engine)
    code_list = code_list['dart_code'].dropna()
    return code_list


def fetch_dart_code():
    """
    매일 돌아가면서 DART에서 코드를 리스팅할 함수
    """

    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': os.environ.get('DART_API_KEY')}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }

    response = requests.get(url, params=params, headers=headers, verify = False).content

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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }

    response = requests.get(url, params = params, headers=headers).json()

    if response['status'] == '000' : 
        return pd.DataFrame(response['list'])
    else :
        return pd.DataFrame()

def get_recent_report(CORP_CODE, now) : 

    # 1. DART AI 요청
    bgn_de = (now - relativedelta(month=6)).strftime('%Y%m%d')
    url = 'https://opendart.fss.or.kr/api/list.json'
    params = {
        'crtfc_key': os.environ.get('DART_API_KEY'),
        'corp_code' : CORP_CODE,
        'bgn_de' : bgn_de,
        'pblntf_ty' : 'A',
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        }
    response = requests.get(url, params = params, headers=headers).json()
    
    # [예외처리] 정상이 아닌 경우 Pass
    if response['status'] != '000' : 
        return None, None

    # 2. 최근 보고서의 기준년도 추출
    data = response['list']
    date = data[0]['report_nm'].strip()
    YEAR = re.sub('[^0-9]+', "", date)[:4]

    # 3. 최근 보고서의 구분번호 추출
    REPORT_NO = {
        '1분기' : '11013',
        '반기' : '11012',
        '3분기' : '11014',
        '사업' : '11011',
    }

    # 3-1. 데이터 전처리 (기재정정 등이 앞에 붙어 있는 경우)
    # 구분하는 기준 : 6개월 내에 분기, 반기, 사업 중 어느 데이터가 들어와 있는지
    report_name = [item['report_nm'] for item in data]
    report_name = [re.sub('\[\D+\]', '', item)[:2] for item in report_name]
    recent_report = report_name[0]

    if recent_report != '분기' :
        return YEAR, REPORT_NO[recent_report]
    else :
        if "사업" in report_name :
            return YEAR, REPORT_NO["1분기"]
        else :
            return YEAR, REPORT_NO["3분기"]
