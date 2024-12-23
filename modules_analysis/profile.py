import pandas as pd
import numpy as np

"""
종목별로 테마, 최근 수익률, 특징, 유사 ETF 등을 기록해두는 코드
"""

# +---------------------------+
# |                           |
# |      ETF 유사도 계산       |
# |                           |
# +---------------------------+
def get_etf_similarity(engine):

    data = pd.read_sql('SELECT * FROM new_data', engine)
    sim_data = data[['etf_code', 'stock_code', 'ratio']]
    pivot = sim_data.pivot_table(index='stock_code', columns='etf_code', values='ratio', aggfunc='sum')
    pivot = pivot.fillna(0)

    pivot_T = pivot.T

    cosine_sim_matrix = np.zeros((pivot_T.shape[0], pivot_T.shape[0]))

    # 코사인 유사도 계산
    for i in range(pivot_T.shape[0]):
        for j in range(pivot_T.shape[0]):
            cosine_sim_matrix[i, j] = get_cosine_similarity(pivot_T.iloc[i].values, pivot_T.iloc[j].values)

    cosine_sim_df = pd.DataFrame(cosine_sim_matrix, index=pivot_T.index, columns=pivot_T.index)
    sim_dict = {}
    for code in pivot_T.index:
        sim_etfs_top5 = cosine_sim_df[code].sort_values(ascending=False).head(6)
        sim_etfs_top5 = sim_etfs_top5.index.tolist()
        sim_etfs_top5.remove(code)
        sim_dict[code] = sim_etfs_top5

    data = pd.DataFrame(sim_dict)
    data.to_sql('similar_etf', con = engine, if_exists='replace', index=False)

    return data

def get_cosine_similarity(vec1, vec2):
    dot_product = np.dot(vec1, vec2)
    norm_vec1 = np.linalg.norm(vec1)
    norm_vec2 = np.linalg.norm(vec2)
    return dot_product / (norm_vec1 * norm_vec2)