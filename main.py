from modules_data.database import *
from modules_data.info import *
from modules_data.research import *
from modules_data.krx import *
from modules_data.etf import *
from modules_data.dart import *
from modules_analysis.price import *
from modules_analysis.profile import *
from config.config import *

if __name__ == '__main__' :
 
    # DB 엔진 생성
    engine = create_db_engine()

    # [작업1] 기본 정보 업데이트
    print('[작업1] 기본 정보 업데이트')
    update_basic_information(engine)

    # [작업2] 이전 일자 ETF 포트폴리오
    print('[작업2] 이전 일자 ETF 포트폴리오')
    update_krx_etf_data(old_date, 'old_data', engine)

    # [작업3] 최근 일자 ETF 포트폴리오
    print('[작업3] 최근 일자 ETF 포트폴리오')
    update_krx_etf_data(new_date, 'new_data', engine)

    # [작업4] 증권사 종목 리포트 업데이트
    print('[작업4] 증권사 종목 리포트 업데이트')
    research = update_research(engine)

    # [작업5] 증권사 종목 리포트 업데이트
    print('[작업5] 증권사 종목 리포트 업데이트')
    get_stock_target_price(research, engine)
    get_etf_target_price(research, engine)
    
    # [작업6] ETF 정보 테이블 생성
    print('[작업6] ETF 정보 테이블 생성')
    create_etf_report_table(research, engine)
    create_etf_base_table(engine)

    # [작업7] ETF 유사도
    print('[작업7] ETF 유사도')
    get_etf_similarity(engine)

    # # [작업8] 재무제표표
    # create_finance_table(engine)