import pytz
from datetime import datetime, timedelta

# 1. Timezone 설정 : 서울
tz = pytz.timezone('Asia/Seoul')


# 2. 기준일자 설정
"""
new_date : 오늘 일자(yyyy-mm-dd)
old_date : 오늘로부터 7일 전(yyyy-mm-dd)
"""
now = datetime.now(tz)
new_date = now.strftime('%Y%m%d')
old_date = (now - timedelta(days=7)).strftime('%Y%m%d')