from modules_data.database import *
from modules_data.dart import *

if __name__ == '__main__' :
    engine = create_db_engine()
    update_finance_base_table(engine)