import pdb
import pandas as pd
import requests
import zipfile
from xml.etree.ElementTree import parse
from io import BytesIO
import os


def dart_codeListing():
    """
    매일 돌아가면서 DART에서 코드를 리스팅할 함수
    """

    url = 'https://opendart.fss.or.kr/api/corpCode.xml'
    params = {'crtfc_key': os.environ.get('DART_API_KEY')}

    response = requests.get(url, params=params).content

    with zipfile.ZipFile(BytesIO(response)) as z:
        z.extractall('corpCode')

    xmlTree = parse(os.path.join(os.getcwd(), 'corpCode/CORPCODE.xml'))
    root = xmlTree.getroot()
    raw_list = root.findall('list')

    corp_list = []

    for i in range(0, len(raw_list)):
        corp_code = raw_list[i].findtext('corp_code')
        corp_name = raw_list[i].findtext('corp_name')
        stock_code = raw_list[i].findtext('stock_code')

        corp_list.append([corp_code, corp_name, stock_code])

    dart_code_list = pd.DataFrame(corp_list, columns=['고유번호', '정식명칭', '종목코드'])
    dart_code_list = dart_code_list.loc[dart_code_list['종목코드'] != " ", :]
    
    if not os.path.exists('data') :
        os.makedirs('data')

    return dart_code_list.reset_index(drop=True).to_json('data/dart_code_list.json')


def get_dart_data() :
    url = 'https://raw.githubusercontent.com/SEUNGTO/ETFACE_DATA/refs/heads/main/data/dart_code_list.json'
    dart_code_list = requests.get(url).json()
    dart_code_list = pd.DataFrame(dart_code_list)

    print(dart_code_list)
    pdb.set_trace()

if __name__ == '__main__' : 

    dart_codeListing()
    # get_dart_data()