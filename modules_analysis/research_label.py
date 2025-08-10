import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from config import *

def get_research_label(engine) : 

    q1 = """
        SELECT * 
        FROM research
        """
    research = pd.read_sql(q1, con = engine)
    research['게시일자'] = pd.to_datetime(research['게시일자'])
    research['목표가'] = research['목표가'].astype(float)


    # 월 데이터 생성 : 30일 단위로 자르기
    research['월'] = 0
    today = research['게시일자'].max()
    for i in range(6) :
        end_date = today - relativedelta(days = i * 30)
        start_date = today - relativedelta(days = (i+1) * 30)

        con1 = start_date < research['게시일자'] 
        con2 = research['게시일자'] <= end_date
        research['월'] = np.where(con1 & con2, i, research['월'])

    # 리포트 개수
    tmp = research[['종목코드', '월', 'nid']]
    cnt_report = research[['종목코드', '월', 'nid']].pivot_table(index = '종목코드', columns = '월', aggfunc="count")
    cnt_report.columns = [i for i in range(6)]
    cnt_report = cnt_report.fillna(0)

    #####################
    #  레이블 생성 시작   #
    #####################
    research_label = pd.DataFrame()

    # RSC_1 : 증권사의 관심을 받기 시작했어요.
    label = '증권사의 관심을 받기 시작했어요.'
    con1 = cnt_report[0] > 0
    con2 = cnt_report.loc[:, 1:5].sum(axis = 1) == 0
    tmp = [[code, label] for code in cnt_report[con1&con2].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_2 : 증권사의 관심에서 멀어졌어요.
    label = '증권사의 관심에서 멀어졌어요.'
    con1 = cnt_report[0] == 0
    con2 = cnt_report.loc[:, 1:3].sum(axis = 1) > 0
    tmp = [[code, label] for code in cnt_report[con1&con2].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_3 : 증권사의 관심이 늘었어요.
    label = '증권사의 관심이 늘었어요.'
    con1 = cnt_report[0] > cnt_report.loc[:, 1:2].mean(axis = 1)
    con2 = cnt_report.loc[:, 1:2].mean(axis = 1) > 0
    tmp = [[code, label] for code in cnt_report[con1&con2].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_4 : 증권사의 관심이 줄었어요.
    label = '증권사의 관심이 줄었어요.'
    con1 = cnt_report[0] < cnt_report.loc[:, 1:2].mean(axis = 1)
    con2 = cnt_report.loc[:, 1:2].mean(axis = 1) > 0
    con3 = cnt_report[0] > 0
    tmp = [[code, label] for code in cnt_report[con1&con2&con3].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_5 : 여러 애널리스트들의 관심을 받고 있어요.

    # 증권사 개수 테이블 생성
    cnt_broker = research[['종목코드', '월', '증권사']].drop_duplicates()
    cnt_broker = cnt_broker.pivot_table(index = '종목코드', columns = '월', aggfunc = 'count')
    cnt_broker = cnt_broker.fillna(0)
    cnt_broker.columns = [i for i in range(6)]

    label = '여러 애널리스트들의 관심을 받고 있어요'
    con1 = cnt_broker[0] > 1
    tmp = [[code, label] for code in cnt_broker[con1].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])


    # RSC_6 ~ 8
    opinion = research[['종목코드', '증권사', '게시일자', '의견', '월', '목표가']].sort_values(['종목코드', '증권사', '게시일자'])
    opinion.loc[:, '이전목표가'] = opinion.groupby(['종목코드', '증권사']).shift(1)['목표가']

    # 최초 의견인 경우 중립 의견으로 만들도록 기존 목표가로 대체
    idx = opinion['이전목표가'].isna()
    opinion.loc[idx, '이전목표가'] = opinion.loc[idx, '목표가']
    opinion.loc[:, '차이'] = opinion['목표가']/opinion['이전목표가']

    # 목표가를 5% 이상 올렸다면 상향, 5% 이상 내렸다면 하향, 그렇지 않다면 중립
    opinion.loc[:, '상하향의견'] = np.where(opinion['차이'] > 1.05, 1.0, np.where(opinion['차이'] < 0.95, -1.0, 0.0))
    opinion['강도'] = opinion['의견'].replace({

        'StrongBuy' : 1.2,
        'OutPerform' : 1.2,
        '매도' : 1.2,
        'Buy' : 1.0,
        '매수' : 1.0,   
        '없음' : 0.5,
        'Hold' : 0.5,
        'MarketPerform' : 0.5,
        '중립' : 0,
        'Neutral' : 0,   
    }).infer_objects(copy=False)
    opinion.loc[:, '평가'] = opinion['강도'] * opinion['상하향의견']

    opinion = opinion.loc[opinion['월'] == 0, ['종목코드', '평가']]
    opinion = opinion.groupby('종목코드').mean().reset_index()

    tmp = [
        [code, '목표가가 상향되었어요.'] if v > 1/3 else 
        [code, '목표가가 하향되었어요.'] if v < -1/3 else 
        [code, '목표가에 큰 변화는 없어요.'] for code, v in zip(opinion['종목코드'], opinion['평가'])
        ]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_9 : 목표가가 신고가를 경신했어요.
    label = '목표가가 신고가를 경신했어요.'
    tmp = research[['종목코드', '월', '목표가']].pivot_table(index = '종목코드', columns = '월', aggfunc = 'max')
    tmp.columns = [i for i in range(6)]

    con1 = tmp[0] > tmp.loc[:, 1:6].max(axis = 1)
    con2 = ~tmp.loc[:, 1:6].isna().all(axis = 1)
    tmp = [[code, label] for code in tmp[con1&con2].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_10 : 가장 낮은 목표가가 제시됐어요.
    label = '가장 낮은 목표가가 제시됐어요.'
    tmp = research[['종목코드', '월', '목표가']].pivot_table(index = '종목코드', columns = '월', aggfunc = 'min')
    tmp.columns = [i for i in range(6)]

    con1 = tmp[0] < tmp.loc[:, 1:6].min(axis = 1)
    con2 = ~tmp.loc[:, 1:6].isna().all(axis = 1)

    tmp = [[code, label] for code in tmp[con1&con2].index]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])

    # RSC_11 : 매도리포트가 나왔어요.
    label = "매도리포트가 나왔어요."
    con1 = research['의견'].isin(['매도', 'Sell'])
    con2 = research['월'] == 0
    tmp = [[code, label] for code in research[con1 & con2]['종목코드']]
    tmp = pd.DataFrame(data = tmp, columns = ['종목코드', '레이블'])
    research_label = pd.concat([research_label, tmp])


    # 최종 저장
    research_label = research_label.dropna()
    research_label.to_sql('research_label', engine, if_exists='replace', index = False)