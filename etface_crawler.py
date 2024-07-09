import io
import re
import os
import time
import pytz
from datetime import datetime, timedelta
import FinanceDataReader as fdr
import requests
from bs4 import BeautifulSoup
import zipfile
from google.cloud import storage
from google.oauth2.service_account import Credentials
import oracledb
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT


class Main:
    def __init__(self):
        self.tz = pytz.timezone('Asia/Seoul')
        self.now = datetime.now(self.tz)
        self.new_date = self.now.strftime('%Y%m%d')
        self.old_date = (self.now - timedelta(days=7)).strftime('%Y%m%d')

        # DB 연결
        self.engine = self.create_db_engine()

        # [작업1] 코드 업데이트
        print('[작업1] 코드 업데이트')
#        self.code_list = self.update_code_list()

        # [작업2] 새 데이터
        print('[작업2] 새 데이터')
#        self.krx_code = self.load_KRX_code()
#        self.new_data = self.get_krx_etf_data(self.krx_code, self.new_date)

        # [작업3] 예전 데이터
        print('[작업3] 예전 데이터')
 #       self.old_data = self.get_krx_etf_data(self.krx_code, self.old_date)

        # [작업4] 리서치 데이터 업데이트
        print('[작업4] 리서치 데이터 업데이트')
#        self.research = self.update_research()

        # [작업5] 종목 목표가 계산
        print('[작업5] 종목 목표가 계산')
#        self.stock_target = self.calcurate_target_price(self.research)

        # [작업6] ETF 목표가 계산
        print('[작업6] ETF 목표가 계산')
#        self.etf_target = self.calcurate_etf_target_price(self.research)

        # [작업7] 유사 종목 계산
        print('[작업7] 유사 종목 계산')
#        self.similar = self.compute_similarity()

        # [작업8] ETF 기본 테이블 생성
        print('[작업8] ETF 기본 테이블 생성')
        self.etf_base_table = self.make_etf_base_table()

        # [작업9] ETF 종목 세부사항 테이블 생성
        print('[작업9] ETF 종목 세부사항 테이블 생성')
        self.etf_deposit_detail = self.make_etf_deposit_detail()

    # +---------------------------+
    # |   함수 정의 영역            |
    # +---------------------------+
    def create_db_engine(self):
        STORAGE_NAME = os.environ.get('STORAGE_NAME')
        WALLET_FILE = os.environ.get('WALLET_FILE')

        test = {
            "type": os.environ.get('GCP_TYPE'),
            "project_id": os.environ.get('GCP_PROJECT_ID'),
            "private_key_id": os.environ.get('GCP_PRIVATE_KEY_ID'),
            "private_key": os.environ.get('GCP_PRIVATE_KEY').replace('\\n', '\n'),
            "client_email": os.environ.get('GCP_CLIENT_EMAIL'),
            "client_id": os.environ.get('GCP_CLIENT_ID'),
            "auth_uri": os.environ.get('GCP_AUTH_URI'),
            "token_uri": os.environ.get('GCP_TOKEN_URI'),
            "auth_provider_x509_cert_url": os.environ.get('GCP_PROVIDER_URL'),
            "client_x509_cert_url": os.environ.get('GCP_CLIENT_URL'),
            "universe_domain": os.environ.get('GCP_UNIV_DOMAIN')
        }

        credentials = Credentials.from_service_account_info(test)
        client = storage.Client(credentials=credentials)
        bucket = client.get_bucket(STORAGE_NAME)
        blob = bucket.get_blob(WALLET_FILE)
        blob.download_to_filename(WALLET_FILE)

        zip_file_path = os.path.join(os.getcwd(), WALLET_FILE)
        wallet_location = os.path.join(os.getcwd(), 'key')
        os.makedirs(wallet_location, exist_ok=True)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(wallet_location)

        connection = oracledb.connect(
            user=os.environ.get('DB_USER'),
            password=os.environ.get('DB_PASSWORD'),
            dsn=os.environ.get('DB_DSN'),
            config_dir=wallet_location,
            wallet_location=wallet_location,
            wallet_password=os.environ.get('DB_WALLET_PASSWORD'))

        engine = create_engine('oracle+oracledb://', creator=lambda: connection)

        return engine

    # 작업1 함수 : 코드 업데이트
    def update_code_list(self):

        stocks = self.load_KRX_code_Stock()
        stocks.loc[:, 'Type'] = 'Stock'

        krx_code = self.load_KRX_code()
        krx_code = krx_code['단축코드'].astype(str)
        etfs = fdr.StockListing('ETF/KR')
        etfs = etfs.loc[:, ['Name', 'Symbol']]
        etfs.loc[:, 'Type'] = 'ETF'
        etfs = etfs[etfs['Symbol'].isin(krx_code)]

        code_list = pd.concat([stocks, etfs])
        code_list.reset_index(drop=True)

        # DB에 반영
        code_list.to_sql('code_list', self.engine, if_exists='replace', index=False)

        return code_list

    def load_KRX_code_Stock(self):
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
                   'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
        otp = requests.post(otp_url, params=otp_params, headers=headers).text
        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down_params = {'code': otp}
        response = requests.post(down_url, params=down_params, headers=headers)
        data = pd.read_csv(io.BytesIO(response.content), encoding='euc-kr', dtype={'단축코드': 'string'})
        data = data[['한글 종목약명', '단축코드']]
        data.columns = ['Name', 'Symbol']
    
        return data

    # 작업2,3 함수 : KRX 데이터 크롤링
    def load_KRX_code(self):

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
               'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}

        otp = requests.post(otp_url, params=otp_params, headers=headers).text

        down_url = 'http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd'
        down_params = {'code': otp}
        response = requests.post(down_url, params=down_params, headers=headers)

        data = pd.read_csv(io.BytesIO(response.content), encoding='euc-kr', dtype={'단축코드': 'string'})
        _filter = (data['기초시장분류'] == '국내') & (data['기초자산분류'] == '주식')
        data = data[_filter]

        return data

    def get_PDF_data(self, isuCd, code, name, date):
        headers = {'Referer': 'http://data.krx.co.kr/contents/MDC/MDI/mdiLoader',
               'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'}
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

    def get_krx_etf_data(self, codeList, date):
        for i, (isuCd, code, name) in enumerate(zip(codeList['표준코드'], codeList['단축코드'], codeList['한글종목약명'])):

            if i == 0:
                data = self.get_PDF_data(isuCd, code, name, date)
                data.insert(0, 'ETF코드', code)
                data = data.drop(['시가총액', '시가총액 구성비중'], axis=1)
                data.loc[:, '비중'] = data['평가금액'] / data['평가금액'].sum() * 100
                time.sleep(0.5)

            else:
                tmp = self.get_PDF_data(isuCd, code, name, date)
                tmp.insert(0, 'ETF코드', code)
                tmp = tmp.drop(['시가총액', '시가총액 구성비중'], axis=1)
                tmp.loc[:, '비중'] = tmp['평가금액'] / tmp['평가금액'].sum() * 100
                data = pd.concat([data, tmp])
                time.sleep(0.5)

        data.columns = ['etf_code', 'stock_code', 'stock_nm', 'stock_amn', 'evl_amt', 'ratio']
        data.reset_index(drop=True)

        table_name = ''
        if date == self.new_date:
            table_name = 'new_data'
        elif date == self.old_date:
            table_name = 'old_data'

        data.to_sql(table_name, self.engine, if_exists='replace', index=False,
                    dtype={
                        'etf_code': String(12),
                        'stock_code': String(12),
                        'stock_nm': String(50),
                        'stock_amn': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'evl_amt': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle')
                    })

        return data

    # 작업4 리서치 데이터 업데이트
    def update_research(self):
        research = self.load_research()
        research = self.clear_old_research(research, 180)

        nid_list = research['nid'].tolist()

        _last_nid = max(nid_list)
        _start_nid = str(int(_last_nid) + 1)
        _recent_nid = self.find_recent_nid()

        new_research = pd.DataFrame([])

        for nid in range(int(_start_nid), int(_recent_nid) + 1):
            time.sleep(0.5)
            nid = str(nid)
            try:
                tmp = pd.DataFrame(self.researchCrawlling(nid))
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
        research.to_sql('research', self.engine, if_exists='replace', index=False)

        return research

    def load_research(self):
        research = pd.read_sql('SELECT * FROM research', self.engine)
        return research

    def clear_old_research(self, research, period):
        testee = research[['게시일자', 'nid']]
        tz = pytz.timezone('Asia/Seoul')
        now = datetime.now(tz)
        testee.loc[:, '게시일자'] = pd.to_datetime(testee['게시일자'], format='mixed').dt.tz_localize(tz)

        tt = now - timedelta(days=period)

        nid_list = testee[testee['게시일자'] >= tt]['nid']

        return research.loc[research['nid'].isin(nid_list), :]

    def find_recent_nid(self):
        url = 'https://finance.naver.com/research/company_list.naver?&page=1'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        href = soup.find('div', class_='box_type_m').find_all('a')[1].attrs['href']
        nid = re.sub(r"(.*)([0-9]{5,6})(.*)", "\g<2>", href)

        return nid

    def researchCrawlling(self, nid):
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
        body = soup.find('div', class_='ResearchContent_article__jjmeq')

        info = body.find('div', class_='HeaderResearch_article__j3dPb')
        code = info.find('em', class_='HeaderResearch_code__RmsRt').text
        stock_name = info.find('em', class_='HeaderResearch_tag__7owlF').text
        stock_name = stock_name.replace(code, "")
        title = info.find('h3', class_='HeaderResearch_title__cnBST').text
        researcher = info.find('cite', class_='HeaderResearch_description__qH6Bs').text
        date = info.find('time', class_='HeaderResearch_description__qH6Bs').text

        consensus = body.find('div', class_='ResearchConsensus_article__YZ7oY')
        consensus = consensus.find_all('span', class_='ResearchConsensus_text__XNJAT')
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

    # [작업5] 종목 목표가 구하기
    def calcurate_ewm(self, researchData):
        researchData['목표가'] = researchData['목표가'].fillna("")
        researchData['목표가'] = [re.sub("\D", "", v) for v in researchData['목표가']]
        researchData = researchData[researchData['목표가'] != ""]
        researchData.loc[:, '게시일자'] = researchData['게시일자'].apply(lambda x: x.replace(".", ""))
        researchData.loc[:, '게시일자'] = pd.to_datetime(researchData.loc[:, '게시일자'])
        researchData.loc[:, '목표가'] = researchData.loc[:, '목표가'].astype(float)

        pivot = researchData.pivot_table(index='게시일자', columns='종목코드', values='목표가', aggfunc='mean')
        pivot = pivot.astype(float)

        start = researchData.loc[:, '게시일자'].min()
        end = researchData.loc[:, '게시일자'].max()

        period = pd.date_range(start=start, end=end, freq='D')
        bs_data = pd.DataFrame([], index=period)
        bs_data = bs_data.merge(pivot, left_index=True, right_index=True)

        ewmdata = bs_data.ewm(span=90, adjust=False).mean()
        ewmdata.index = ewmdata.index.astype(str)
        ewmdata.reset_index(inplace=True)
        ewmdata = ewmdata.rename(columns={'index': 'Date'})

        return ewmdata

    def calcurate_target_price(self, researchData):
        ewmdata = self.calcurate_ewm(researchData)
        ewmdata = ewmdata.bfill()
        ewmdata = pd.melt(ewmdata, id_vars=['Date'])
        ewmdata.columns = ['Date', 'code', 'target']
        ewmdata.to_sql('stock_target', self.engine, if_exists='replace', index=False,
                       dtype={
                           'Date': String(10),
                           'code': String(6),
                           'target': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),
                                                                      'oracle')
                       })

        return ewmdata

    # [작업6] ETF 목표가 계산
    def calcurate_etf_target_price(self, research):

        ewmdata = self.calcurate_ewm(research)

        start = ewmdata[['Date']].min().values[0]
        end = ewmdata[['Date']].max().values[0]

        codeList = ewmdata.columns
        codeList = codeList.drop('Date')

        price_data = pd.DataFrame({})
        for code in codeList:
            tmp = fdr.DataReader(code, start=start, end=end)['Close']
            tmp.bfill(inplace=True)
            tmp.name = code
            price_data = pd.concat([price_data, tmp], axis=1)

        # index 속성 맞추기
        ewmdata.index = ewmdata['Date']
        price_data.index = [str(idx)[:10] for idx in price_data.index]
        ewmdata.fillna(price_data, inplace=True)
        ewmdata.bfill(inplace=True)

        # ETF 데이터 불러오기
        etf_data = pd.read_sql('SELECT * FROM new_data', self.engine)
        
        # 종가를 저장할 데이터 프레임 만들어두기  ## 로드시간 최소화
        stock_mkt_price = pd.DataFrame({})

        # ETF별로 구한 최종 목표가를 저장할 데이터 프레임 생성
        etf_target_price = pd.DataFrame({})

        for etf_code in etf_data['etf_code'].unique():

            tmp = etf_data[etf_data['etf_code'] == etf_code]

            # 목표가가 있는 경우
            in_ewm = [code for code in tmp['stock_code'] if code in ewmdata.columns]
            ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in in_ewm]
            in_ewm_price = ewmdata[in_ewm] * ratio / 100
            in_ewm_price = in_ewm_price.sum(axis=1)

            # 목표가가 없는 경우 -> 종가로 대체
            out_ewm = [code for code in tmp['stock_code'] if code not in ewmdata.columns]
            out_ewm_price = pd.DataFrame({})

            for stock_code in out_ewm:
                if len(re.sub("\d", "", stock_code)) == 0:
                    try:
                        if stock_code in stock_mkt_price.columns:
                            out_ewm_price = pd.concat([out_ewm_price, stock_mkt_price[stock_code]], axis=1)

                        elif stock_code not in stock_mkt_price.columns:
                            tmp_stock_price = fdr.DataReader(stock_code, start=start, end=end)
                            tmp_stock_price.index = [str(idx)[:10] for idx in tmp_stock_price.index]
                            tmp_stock_price = tmp_stock_price['Close']
                            tmp_stock_price.bfill(inplace=True)
                            tmp_stock_price.name = stock_code

                            out_ewm_price = pd.concat([out_ewm_price, tmp_stock_price], axis=1)

                            # stock_mkt_price에도 저장
                            stock_mkt_price = pd.concat([stock_mkt_price, tmp_stock_price], axis=1)

                    except:
                        continue
                else:
                    continue

            # 검색되는 경우만 load
            ratio = [tmp.loc[tmp['stock_code'] == stock_code, 'ratio'].values[0] for stock_code in
                     out_ewm_price.columns]

            out_ewm_price = out_ewm_price * ratio / 100
            out_ewm_price = out_ewm_price.sum(axis=1)

            # 합치기
            final = sum(out_ewm_price, in_ewm_price)
            final.name = etf_code

            etf_target_price = pd.concat([etf_target_price, final], axis=1)

        etf_target_price = etf_target_price.bfill()
        etf_target_price.reset_index(inplace=True)
        etf_target_price = etf_target_price.rename(columns={'index': 'Date'})
        etf_target_price = pd.melt(etf_target_price, id_vars=['Date'])
        etf_target_price.columns = ['Date', 'code', 'target']
        etf_target_price.to_sql('etf_target', self.engine,
                                if_exists='replace', index=False,
                                dtype={
                                    'Date': String(10),
                                    'code': String(6),
                                    'target': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),
                                                                               'oracle')
                                })

        return etf_target_price

    # [작업7] 유사 종목 계산
    def cosine_similarity_manual(self, vec1, vec2):
        dot_product = np.dot(vec1, vec2)
        norm_vec1 = np.linalg.norm(vec1)
        norm_vec2 = np.linalg.norm(vec2)
        return dot_product / (norm_vec1 * norm_vec2)

    def compute_similarity(self):
        data = pd.read_sql('SELECT * FROM new_data', self.engine)

        sim_data = data[['etf_code', 'stock_code', 'ratio']]
        pivot = sim_data.pivot_table(index='stock_code', columns='etf_code', values='ratio', aggfunc='sum')
        pivot = pivot.fillna(0)

        pivot_T = pivot.T

        cosine_sim_matrix = np.zeros((pivot_T.shape[0], pivot_T.shape[0]))

        # 코사인 유사도 계산
        for i in range(pivot_T.shape[0]):
            for j in range(pivot_T.shape[0]):
                cosine_sim_matrix[i, j] = self.cosine_similarity_manual(pivot_T.iloc[i].values, pivot_T.iloc[j].values)

        cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=pivot_T.index, columns=pivot_T.index)
        sim_dict = {}
        for code in pivot_T.index:
            sim_etfs_top5 = cosine_sim_df[code].sort_values(ascending=False).head(6)
            sim_etfs_top5 = sim_etfs_top5.index.tolist()
            sim_etfs_top5.remove(code)
            sim_dict[code] = sim_etfs_top5

        data = pd.DataFrame(sim_dict)

        data.to_sql('similar_etf', self.engine, if_exists='replace', index=False)

        return data

    # [작업8] ETF 기본 테이블 생성
    def make_etf_base_table(self):

        etf_name = fdr.StockListing('ETF/KR')[['Symbol', 'Name']]
        etf_name.columns = ['etf_code', 'etf_name']

        data = pd.read_sql('SELECT * FROM new_data', self.engine)
        data.columns = ['etf_code', 'stock_code', 'stock_name', 'recent_quantity', 'recent_amount', 'recent_ratio']

        data2 = pd.read_sql('SELECT * FROM old_data', self.engine)
        data2.columns = ['etf_code', 'stock_code', 'stock_name', 'past_quantity', 'past_amount', 'past_ratio']

        data = data.merge(data2, how='outer', on=['etf_code', 'stock_code'])
        data = data.merge(etf_name, how='left', on='etf_code')

        data['stock_name_x'] = data['stock_name_x'].fillna(data['stock_name_y'])
        data.drop(['stock_name_y'], axis=1, inplace=True)
        data = data.rename(columns={'stock_name_x': 'stock_name'})
        data.fillna(0, inplace=True)

        data = data[['etf_code', 'etf_name', 'stock_code', 'stock_name',
                     'recent_quantity', 'recent_amount', 'recent_ratio',
                     'past_quantity', 'past_amount', 'past_ratio']]
        data['diff_ratio'] = data['recent_ratio'] - data['past_ratio']

        data.to_sql('etf_base_table', self.engine, if_exists='replace', index=False,
                    dtype={
                        'etf_code': String(12),
                        'etf_name': String(50),
                        'stock_code': String(12),
                        'stock_name': String(50),
                        'recent_quantity': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),
                                                                            'oracle'),
                        'recent_amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'recent_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'past_quantity': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'past_amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'past_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'diff_ratio': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    })

        return data

    # [작업9] ETF 종목 세부사항 테이블 생성
    def make_etf_deposit_detail(self):

        def str_to_float(s):
            if s == "":
                return None
            elif s == "없음":
                return None
            elif s == None:
                return None
            try:
                return float(s)
            except ValueError:
                return None

        data = pd.read_sql('SELECT * FROM research', self.engine)

        data['목표가'] = data['목표가'].apply(str_to_float)
        target_price = data.groupby('종목코드')['목표가'].mean().reset_index()

        idx = data.groupby('종목코드')['nid'].idxmax()
        data = data.loc[idx, :]

        data = data.drop('목표가', axis=1).merge(target_price, how='outer', on='종목코드')
        data.drop('nid', axis=1, inplace=True)

        data.columns = ['stock_name', 'stock_code', 'report_title', 'report_opinion', 'report_pubdate',
                        'report_researcher',
                        'report_link', 'stock_target_price']
        data = data[['stock_name', 'stock_code', 'stock_target_price',
                     'report_title', 'report_opinion', 'report_pubdate', 'report_researcher', 'report_link']]

        data.to_sql('etf_deposit_detail', self.engine, if_exists='replace', index=False,
                    dtype={
                        'stock_name': String(50),
                        'stock_code': String(12),
                        'stock_target_price': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),
                                                                               'oracle'),
                        'report_title': String(100),
                        'report_opinion': String(50),
                        'report_pubdate': String(20),
                        'report_researcher': String(50),
                        'report_link': String(100)
                    })
        return data


if __name__ == '__main__':

    app = Main()
