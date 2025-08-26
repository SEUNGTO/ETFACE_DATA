import pandas as pd
from sqlalchemy import Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT
import pytz
from datetime import datetime


def get_theme_label(engine) : 

    theme = pd.read_sql('SELECT * FROM theme', con = engine)
    research_label = pd.read_sql('SELECT * FROM research_label', con = engine)

    theme = research_label.set_index('종목코드').join(theme.set_index('종목코드'))
    theme = theme.reset_index()
    theme = theme[['테마코드', '테마명', '종목코드', '종목명', '레이블']]
    theme = theme.sort_values(['테마명', '레이블'])
    theme.loc[:, '점수'] = theme['레이블'].replace({
        '증권사의 관심을 받기 시작했어요.' : 0.5,
        '증권사의 관심이 늘었어요.' : 0.8,
        '목표가가 상향되었어요.' : 0.9,
        '목표가가 신고가를 경신했어요.' : 1.0,
        '여러 애널리스트들의 관심을 받고 있어요.' : None,

        '목표가에 큰 변화는 없어요.' : 0.0,

        '증권사의 관심에서 멀어졌어요.' : -0.5,
        '증권사의 관심이 줄었어요.' : -0.8,
        '목표가가 하향되었어요.'  : -0.9,
        '가장 낮은 목표가가 제시됐어요.' : -1.0,
        '매도리포트가 나왔어요.' : -1.0,
    })

    sentiment = theme[['테마코드','점수']].groupby('테마코드').mean()
    sentiment = sentiment.reset_index()
    sentiment.loc[:, '레이블'] = [
        '긍정' if v > 1/3 else
        '부정' if v < -1/3 else
        '중립' for v in sentiment['점수']
    ]
    sentiment.columns = ['테마코드', '테마점수', '테마레이블']

    theme = theme.set_index('테마코드').join(sentiment.set_index('테마코드'))
    theme = theme.reset_index()
    theme = theme.dropna()
    theme.to_sql('theme_label',
                con = engine,
                if_exists='replace',
                index = False,
                dtype = {
                    '점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                    '테마점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                })

    # daily data에 추가
    tz = pytz.timezone('Asia/Seoul')
    now = datetime.now(tz)
    
    theme['날짜'] = now.strftime('%Y-%m-%d')
    theme.to_sql(
        'theme_label_daily',
        con = engine,
        if_exists='append',
        index = False,
        dtype = {
            '점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
            '테마점수' : Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
        }
    )