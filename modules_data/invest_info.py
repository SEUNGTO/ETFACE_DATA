import re
import pytz
import time
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def update_invest_info(engine, day:int) :

    invest_info = load_invest_info(engine)
    invest_info = clean_old_invest_info(invest_info, day)
    
    nid_list = invest_info['nid'].tolist()
    _last_nid = max(nid_list)
    _start_nid = int(_last_nid) + 1
    _recent_id = find_recent_invest_nid()
    
    new_data = pd.DataFrame()
    
    for nid in range(_start_nid, _recent_id+1) :
        time.sleep(np.random.rand())

        try : 
            tmp = fetch_invest_info_report(nid)
            
            if tmp.empty : # 중간에 삭제되어 nid가 비어있는 경우 >> 다음으로
                continue
            else : 
                new_data = pd.concat([new_data, tmp])

        except : 
            continue
        
    if new_data.empty : # 새로 쌓인 데이터가 없는 경우
        pass

    else : 
        invest_info = pd.concat([invest_info, new_data])
        invest_info.loc[:, 'nid'] = invest_info.loc[:, 'nid'].astype(int)
        invest_info.to_sql('invest_info', con = engine, if_exists='replace', index = False)

def load_invest_info(engine) -> pd.DataFrame: 
    invest_info = pd.read_sql('SELECT * FROM invest_info', con = engine)
    return invest_info

def clean_old_invest_info(invest_info:pd.DataFrame, period:int) -> pd.DataFrame : 
    testee = invest_info[['게시일자', 'nid']]
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    testee.loc[:, '게시일자'] = pd.to_datetime(testee['게시일자'], format='mixed').dt.tz_localize(tz)

    tt = now - timedelta(days=period)

    nid_list = testee[testee['게시일자'] >= tt]['nid']

    return invest_info.loc[invest_info['nid'].isin(nid_list), :]

def find_recent_invest_nid() -> int : 
    url = 'https://finance.naver.com/research/invest_list.nhn?&page=1'
    response = requests.get(url)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')
    href = soup.find('div', {'class' : 'box_type_m'}).find_all('a')[0]['href']
    nid = re.sub(r"(.*)([0-9]{5,6})(.*)", "\g<2>", href)
    
    return int(nid)

def fetch_invest_info_report(nid:int) -> pd.DataFrame :

    url = f'https://finance.naver.com/research/invest_read.naver?nid={nid}'
    response = requests.get(url)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')


    content = soup.find('th', {'class' : re.compile('view_sbj')})
    if content : 

        broker_info = content.find('p', {'class' : re.compile('source')}).text
        broker, date, _ = broker_info.split("|")
        
        content.find('p', {'class' : re.compile('source')}).decompose()
        title = content.text.strip()
        link = f"https://m.stock.naver.com/investment/research/invest/{nid}"
        
        down_link = soup.find('th', {'class' : 'view_report'}).find('a')['href']
        
        data = {
            'nid' : nid,
            '제목' : title,
            '증권사' : broker, 
            '게시일자' : date, 
            '링크' : link,
            '다운로드' : down_link,
        }
        return pd.DataFrame(data, index = [0])