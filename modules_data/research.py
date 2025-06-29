import re
import time
import pytz
import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


def update_research(engine):

    research = load_research(engine)
    research = clear_old_research(research, 180)

    nid_list = research['nid'].tolist()

    _last_nid = max(nid_list)
    _start_nid = str(int(_last_nid) + 1)

    _recent_nid = find_recent_nid()

    new_research = pd.DataFrame([])

    for nid in range(int(_start_nid), int(_recent_nid) + 1):
        time.sleep(np.random.rand())
        nid = str(nid)
        try:
            tmp = pd.DataFrame(fetch_research(nid))
            new_research = pd.concat([new_research, tmp])
        except:
            continue
    if new_research.shape[0] != 0:
        new_research.columns = ['종목명', '종목코드', '리포트 제목', 'nid', '목표가', '의견', '게시일자', '증권사', '링크']
    else:
        new_research = pd.DataFrame([], columns=['종목명', '종목코드', '리포트 제목', 'nid', '목표가', '의견', '게시일자', '증권사', '링크'])

    research = pd.concat([research, new_research])
    research = research.reset_index(drop=True)

    # DB 업데이트
    research.to_sql('research', engine, if_exists='replace', index=False)

    return research

def load_research(engine):
    research = pd.read_sql('SELECT * FROM research', con = engine)
    return research


def clear_old_research(research, period):
    testee = research[['게시일자', 'nid']]
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    testee.loc[:, '게시일자'] = pd.to_datetime(testee['게시일자'], format='mixed').dt.tz_localize(tz)

    tt = now - timedelta(days=period)

    nid_list = testee[testee['게시일자'] >= tt]['nid']

    return research.loc[research['nid'].isin(nid_list), :]

def find_recent_nid():
    url = 'https://finance.naver.com/research/company_list.naver?&page=1'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    href = soup.find('div', class_='box_type_m').find_all('a')[1].attrs['href']
    nid = re.sub(r"(.*)([0-9]{5,6})(.*)", "\g<2>", href)

    return nid


def fetch_research(nid):
    result = {
        'stock_name': [],
        'code': [],
        'title': [],
        'nid': [],
        'target_price': [],
        'opinion': [],
        'date': [],
        'researcher': [],
        'link': []
    }

    link = f'https://m.stock.naver.com/investment/research/company/{nid}'
    response = requests.get(link)
    soup = BeautifulSoup(response.content, 'html.parser')
    body = soup.find('div', {'class' : re.compile('ResearchContent_article')})

    if not body :
        raise ValueError


    info = body.find('div', {'class' : re.compile('ResearchHeader_article')})
    code = info.find('em', {'class' : re.compile('ResearchHeader_code')}).text
    stock_name = info.find('em', {'class' : re.compile('ResearchHeader_tag')}).text
    stock_name = stock_name.replace(code, "")
    title = info.find('h3',{'class' : re.compile('ResearchHeader_title')}).text
    researcher = info.find('cite', {'class' : re.compile('ResearchHeader_description')}).text
    date = info.find('time', {'class' : re.compile('ResearchHeader_description')}).text

    consensus = body.find('div', {'class' : re.compile('ResearchConsensus_article')})
    consensus = consensus.find_all('div', {'class' : re.compile('ResearchConsensus_text')})
    opinion = consensus[0].text
    target_price = re.sub("\D", "", consensus[1].text)

    result['stock_name'].append(stock_name)
    result['code'].append(code)
    result['title'].append(title)
    result['nid'].append(nid)
    result['researcher'].append(researcher)
    result['date'].append(date)
    result['target_price'].append(target_price)
    result['opinion'].append(opinion)
    result['link'].append(link)

    return result
