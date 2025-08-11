from modules_data.database import *
from modules_data.info import *
from modules_data.research import *
from modules_data.krx import *
from modules_data.dart import *
from modules_data.industry import *
from modules_data.theme import *
from modules_analysis.etf import *
from modules_analysis.price import *
from modules_analysis.profile import *
from modules_analysis.research_label import *
from modules_analysis.industry_label import *
from modules_analysis.theme_label import *
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

    # # [작업5] ETF 재무제표 작성
    print('[작업5] ETF 재무제표 작성')
    update_etf_finance(engine)

    # [작업6] 목표가 계산
    print('[작업6] 목표가 계산')
    get_stock_target_price(research, engine)
    get_etf_target_price(research, engine)
    
    # [작업7] ETF 정보 테이블 생성
    print('[작업7] ETF 정보 테이블 생성')
    create_etf_report_table(research, engine)
    create_etf_base_table(engine)

    # [작업8] ETF 유사도
    print('[작업8] ETF 유사도')
    get_etf_similarity(engine)
    
    # [작업9] 증권사 리포트 레이블
    print('[작업9] 증권사 리포트 레이블')
    get_research_label(engine)

    # [작업10] 업종 데이터(WICS)
    print('[작업10] 업종 데이터(WICS)')
    get_industry_info(engine)

    # [작업11] 업종별 레이블
    print('[작업11] 업종별 레이블')
    get_industry_label(engine)

    # [작업12] 테마 데이터(네이버)
    print('[작업12] 테마 데이터')
    get_theme_info(engine)

    # [작업13] 테마 데이터(네이버)
    print('[작업13] 테마별 레이블')
    get_theme_label(engine)

