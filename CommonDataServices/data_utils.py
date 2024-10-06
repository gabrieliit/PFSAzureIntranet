import pandas as pd

def mdb_query_postproc(query_res,dtype='native'):
    #at the moment this just converts the query results to string for display in dash tables
    #query results is output of a call to mongodb collection.find()
    df=pd.DataFrame(query_res)
    if dtype=='str':
        df=df.astype(str)
    return df

