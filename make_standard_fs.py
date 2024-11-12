import pdb
import pandas as pd


def filter_account(fs_data) :

    pass



if __name__ == "__main__" :

    # fs_all = pd.read_excel('data/fs-account.xlsx')
    corp_list = pd.read_excel('data/etf_fs_target_company.xlsx')

    for idx, v in corp_list.iterrows() :
        corp_code, stock_name, stock_code = v


        pdb.set_trace()
    
