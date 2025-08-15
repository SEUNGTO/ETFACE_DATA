import re
import pytz
import time
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def update_industry_report(engine, day:int) :

    industry = load_industry(engine)
    industry = clear_old_industry_report(industry, day)
    
    nid_list = industry['nid'].tolist()
    _last_nid = max(nid_list)
    _start_nid = int(_last_nid) + 1
    
    _recent_id = find_recent_industry_nid()
    
    new_data = pd.DataFrame()
    
    for nid in range(_start_nid, _recent_id+1) :
        time.sleep(np.random.rand())
        
        try : 
            tmp = fetch_industry_report(nid)
            
            if tmp.empty : # 중간에 삭제되어 nid가 비어있는 경우 >> 다음으로
                continue
            else : 
                new_data = pd.concat([new_data, tmp])

        except : 
            continue
        
    if new_data.empty : # 새로 쌓을 데이터가 없는 경우
        pass

    else : 
        industry = pd.concat([industry, new_data])
        industry.loc[:, 'nid'] = industry.loc[:, 'nid'].astype(int)
        industry.to_sql('industry_report', con = engine, if_exists='replace', index = False)

def load_industry(engine) -> pd.DataFrame: 
    industry = pd.read_sql('SELECT * FROM industry_report', con = engine)
    return industry

def clear_old_industry_report(industry:pd.DataFrame, period:int) -> pd.DataFrame : 
    testee = industry[['게시일자', 'nid']]
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    testee.loc[:, '게시일자'] = pd.to_datetime(testee['게시일자'], format='mixed').dt.tz_localize(tz)

    tt = now - timedelta(days=period)

    nid_list = testee[testee['게시일자'] >= tt]['nid']

    return industry.loc[industry['nid'].isin(nid_list), :]

def find_recent_industry_nid() -> int : 
    url = 'https://finance.naver.com/research/industry_list.nhn?&page=1'
    response = requests.get(url)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')
    href = soup.find('div', {'class' : 'box_type_m'}).find_all('a')[0]['href']
    nid = re.sub(r"(.*)([0-9]{5,6})(.*)", "\g<2>", href)
    
    return int(nid)

def fetch_industry_report(nid:int) -> pd.DataFrame :

    url = f'https://finance.naver.com/research/industry_read.naver?nid={nid}'
    response = requests.get(url)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')

    content = soup.find('th', {'class' : re.compile('view_sbj')})
    if content : 
   
        broker_info = content.find('p', {'class' : re.compile('source')}).text
        broker, date, _ = broker_info.split("|")
        
        industry = content.find('em').text
        content.find('em').decompose()
        content.find('p', {'class' : re.compile('source')}).decompose()
        title = content.text.strip()
        person = soup.find('em', {'class' : re.compile('person')}).text

        link = f"https://m.stock.naver.com/investment/research/industry/{nid}"
        down_link = soup.find('th', {'class' : 'view_report'}).find('a')['href']

        
        data = {
            'nid' : nid,
            '산업' : industry,
            '제목' : title,
            '증권사' : broker, 
            '게시일자' : date, 
            '애널리스트명' : person,   
            '링크' : link,
            '다운로드' : down_link,
        }
        
        return pd.DataFrame(data, index = [0])