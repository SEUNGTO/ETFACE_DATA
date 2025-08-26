import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

def get_theme_info(engine) : 
    # get last page
    url = 'https://finance.naver.com/sise/theme.naver'
    headers = {
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
        }
    response = requests.get(url, headers=headers)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')
    last_page = soup.find('td', {'class' : re.compile('pgRR')})
    last_page = last_page.find('a')['href'].split("=")[-1]
    last_page = int(last_page)

    theme_list = []
    for page in range(last_page) :

        url = f'https://finance.naver.com/sise/theme.naver?&page={page+1}'
        response = requests.get(url, headers=headers)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        theme_page = soup.find_all('td', {'class' : re.compile('col_type1')})
        
        for t in theme_page :
            code = t.find('a')['href'].split("=")[-1]
            name = t.find('a').text.strip()
            theme_list.append((code, name))
        
        # 크롤링 차단 방지 쿨다운
        time.sleep(0.5)

    theme = []
    for code, name in theme_list :
        
        url = f'https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={code}'
        response = requests.get(url, headers=headers)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        row_list = soup.find_all('td', {'class' : re.compile('name')})
        
        for row in row_list : 
            stock_code = row.find('a')['href'].split("=")[-1]
            stock_name = row.find('a').text.strip()
            
            theme.append((code, name, stock_code, stock_name))

        # 크롤링 차단 방지 쿨다운
        time.sleep(0.5)

    theme = pd.DataFrame(theme, columns = ['테마코드', '테마명', '종목코드', '종목명'])
    theme['테마명'] = theme['테마명'].str.replace("/", ",")
    theme.to_sql('theme', con = engine, if_exists='replace', index = False)