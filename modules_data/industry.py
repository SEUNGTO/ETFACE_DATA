import re
import requests
import pandas as pd
from bs4 import BeautifulSoup


def get_industry_info(engine) : 
    """
    네이버주식에서 업종코드, 업종명을 크롤링하는 함수
    """
    url = 'https://finance.naver.com/sise/sise_group.naver?type=upjong'
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36',
    }
    response = requests.get(url, headers=headers)
    response.encoding = 'euc-kr'
    soup = BeautifulSoup(response.text, 'html.parser')

    industry_page = soup.find_all('td', {'style' : re.compile('padding-left:10px;')})
    industry = []

    for v in industry_page :
        
        code = v.find('a')['href'].split("=")[-1]
        name = v.text.strip()
        industry.append([code, name])

    industry = pd.DataFrame(industry, columns = ['업종코드', '업종명'])

    wics = pd.DataFrame()

    for _, v in industry.iterrows() :
        code = v['업종코드']
        name = v['업종명']

        url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=upjong&no={code}"
        response = requests.get(url, headers=headers)
        response.encoding = 'euc-kr'
        soup = BeautifulSoup(response.text, 'html.parser')
        
        stock_list = soup.find_all('div', {'class' : 'name_area'})

        buffer = []
        for stock in stock_list :
            stock_code = stock.find('a')['href'].split("=")[-1]
            stock_name = stock.find('a').text
            buffer.append([code, name, stock_code, stock_name])
        
        buffer = pd.DataFrame(buffer, columns = ['업종코드', '업종명', '종목코드', '종목명'])
        wics = pd.concat([wics, buffer])

    wics.to_sql('wics', engine, if_exists='replace')
    