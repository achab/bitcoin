import numpy as np

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