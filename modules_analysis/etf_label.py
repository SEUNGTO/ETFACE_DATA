import pandas as pd
from sqlalchemy import Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def get_etf_label(engine) : 

    research_label = pd.read_sql('SELECT * FROM research_label', con = engine)
    research_label.loc[:, '점수'] = research_label['레이블'].replace({
        '증권사의 관심을 받기 시작했어요.' : 0.5,
        '증권사의 관심이 늘었어요.' : 0.8,
        '목표가가 상향되었어요.' : 0.9,
        '목표가가 신고가를 경신했어요.' : 1.0,
        '여러 애널리스트들의 관심을 받고 있어요' : None,

        '목표가에 큰 변화는 없어요.' : 0.0,

        '증권사의 관심에서 멀어졌어요.' : -0.5,
        '증권사의 관심이 줄었어요.' : -0.8,
        '목표가가 하향되었어요.'  : -0.9,
        '가장 낮은 목표가가 제시됐어요.' : -1.0,
        '매도리포트가 나왔어요.' : -1.0,
        })
    research_label = research_label[['종목코드', '점수']].groupby('종목코드').mean()

    etf_base = pd.read_sql('SELECT * FROM etf_base_table', con = engine)
    etf_base = etf_base[['etf_code', 'etf_name', 'stock_code', 'stock_name', 'recent_ratio']]
    etf_base.columns = ['ETF코드', 'ETF명', '종목코드', '종목명', '비중']

    etf_label = etf_base.set_index('종목코드').join(research_label)
    etf_label = etf_label.reset_index()
    etf_label = etf_label.dropna()

    tmp = etf_label.groupby('ETF코드')[['비중']].sum().rename(columns = {'비중' : '조정비중합'})
    etf_label = etf_label.set_index('ETF코드').join(tmp)
    etf_label.loc[:, '조정비중'] = etf_label['비중'] / etf_label['조정비중합']
    etf_label.loc[:, '가중점수'] = etf_label['조정비중'] * etf_label['점수']
    etf_label = etf_label.reset_index()
    etf_label = etf_label.groupby('ETF코드')[['가중점수']].sum()

    etf_label.loc[:, '레이블'] = [
        '긍정' if v > 1/3 else
        '부정' if v < -1/3 else
        '중립' for v in etf_label['가중점수']
    ]
    etf_label = etf_label.join(etf_base[['ETF코드', 'ETF명']].set_index('ETF코드'), how = 'left')
    etf_label = etf_label.reset_index()
    etf_label = etf_label.drop_duplicates()
    etf_label = etf_label[['ETF코드', 'ETF명', '가중점수', '레이블']]
    etf_label.columns = ['ETF코드', 'ETF명', 'ETF점수', 'ETF레이블']
    etf_label.to_sql(
        'etf_label',
        con = engine,
        if_exists='replace',
        index = False,
        dtype = {
            'ETF점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
        })
    