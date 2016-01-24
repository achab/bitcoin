import numpy as np
import pandas as pd

def stock2return(S):
    return np.diff(np.log(S))

def returnsByYear(year, when='Close'):
    return stock2return(mydata.loc[str(year)+'-01-01':str(year)+'-12-31'][when])

def meanByYear(year, when='Close'):
    tmp = returnsByYear(year,when)
    return np.mean(tmp)

def stdByYear(year):
    tmp = returnsByYear(year,when)
    return np.std(tmp)

def top_K_buyers(data,K=10):
    grouped = data[data.Type == 'buy'].groupby('User_Id')
    btc_vol = grouped.sum()['Bitcoins']
    sorted_buyers = btc_vol.sort_values(ascending=False,inplace=False)
    top_buyers = np.array(sorted_buyers[:K].index)
    return top_buyers

def top_K_sellers(data,K=10):
    grouped = data[data.Type == 'sell'].groupby('User_Id')
    btc_vol = grouped.sum()['Bitcoins']
    sorted_sellers = btc_vol.sort_values(ascending=False,inplace=False)
    top_sellers = np.array(sorted_sellers[:K].index)
    return top_sellers

def buy_sell_matrix(data,top_K=True,K=10):
    unique_users = np.unique(data["User_Id"])
    n_users = len(unique_users)
    def user2index(user_id):
        return np.where(unique_users == user_id.reshape(np.array(user_id).shape[0],1))
    matrix_index = pd.DataFrame(user2index(data["User_Id"])[1], columns=['Matrix index'])
    data = data.join(matrix_index)

    # Compute top K buyers and sellers
    top_buyers = top_K_buyers(data,K)
    buyers2index = {}
    for i,x in enumerate(top_buyers):
        buyers2index[x] = i
    top_sellers = top_K_sellers(data,K)
    sellers2index = {}
    for i,x in enumerate(top_sellers):
        sellers2index[x] = i
    buy_sell_mat = np.zeros((K,K))

    # Loop on the trades
    trades_grouped = data.groupby('Trade_Id')
    for trade, group in trades_grouped:
        ind0 = group.index[0]
        ind1 = group.index[1]
        i = group.loc[ind0]['Matrix index']
        j = group.loc[ind1]['Matrix index']
        if group.loc[ind0]['Type'] == 'buy':
            if i in top_buyers and j in top_sellers:
                i2mat = buyers2index[i]
                j2mat = sellers2index[j]
                buy_sell_mat[i2mat,j2mat] += group.loc[ind0]['Bitcoins']
            elif j in top_buyers and i in top_sellers:
                i2mat = sellers2index[i]
                j2mat = buyers2index[j]
                buy_sell_mat[j2mat,i2mat] += group.loc[ind0]['Bitcoins']

    return buy_sell_mat
