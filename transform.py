import numpy as np

def stock2return(S):
    return np.diff(np.log(S))