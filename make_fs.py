import pdb
import os
import numpy as np
import pandas as pd
import zipfile
import oracledb
from google.cloud import storage
from google.oauth2.service_account import Credentials
from sqlalchemy import create_engine
from sqlalchemy import String, Float
from sqlalchemy.dialects.oracle import FLOAT as ORACLE_FLOAT

def create_db_engine():
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

def choose_report_type(fs) :
    report_types = fs['report_type'].unique()
    if len(report_types) == 1 :
        return fs
    else :
        return fs.loc[fs['report_type'] == 'CFS', :]

def clean_account_name(fs) :

    NI = '당기순이익' # Net Income
    OP = '영업이익' # Opearating Profit

    fs.loc[fs['account_name'].str.contains(NI), 'account_name'] = NI
    fs.loc[fs['account_name'].str.contains(OP), 'account_name'] = OP

    return fs.drop_duplicates()

def clean_fs(fs) :

    data = pd.DataFrame({
        'account_name': ['유동자산', '비유동자산', '자산총계',
                         '유동부채', '비유동부채', '부채총계',
                         '자본금', '이익잉여금', '자본총계',
                         '매출액', '영업이익', '법인세차감전 순이익', '당기순이익']
    }).set_index('account_name')
    fs.set_index('account_name', inplace = True)


    data = data.join(fs)
    col = ['stock_code', 'date', 'report_type', 'fs_type', 'shares']
    data[col] = data[col].ffill()
    data[col] = data[col].bfill()

    return data

def standardize_fs(fs) :

    """
    재무제표를 데이터화 하는 함수
    금융기업은 제외함(금융기업의 경우 유동성 기준 재무제표가 아니므로 '유동자산'계정이 기표되지 않음을 이용)
    """

    if '유동자산' in fs['account_name'].tolist() :

        data = choose_report_type(fs)
        data = clean_account_name(data)
        data = clean_fs(data)

        return data

if __name__ == "__main__" :

    db = create_db_engine()
    data = pd.read_sql("SELECT * FROM STOCK_FS", con = db)

    result = pd.DataFrame({})
    stock_code_list = data['stock_code'].unique()

    for stock_code in stock_code_list :

        fs = data[data['stock_code'] == stock_code]

        if fs is not None :
            print(stock_code, end = "\r")
            fs = standardize_fs(fs)
            result = pd.concat([result, fs])

    result.reset_index(inplace = True)
    result.to_sql('stock_fs', con = db, if_exists="replace", index=False,
                    dtype={
                        'stock_code': String(12),
                        'date': String(12),
                        'account_name': String(20),
                        'report_type': String(5),
                        'fs_type': String(5),
                        'amount': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'shares': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126), 'oracle'),
                        'amount_per_share': Float(precision=53).with_variant(ORACLE_FLOAT(binary_precision=126),
                                                                            'oracle')
                       })