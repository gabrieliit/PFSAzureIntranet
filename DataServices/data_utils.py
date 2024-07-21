import pandas as pd
def mdb_query_postproc(query_res):
    #at the moment this just converts the query results to string for display in dash tables
    #query results is output of a call to mongodb collection.find()
    df=pd.DataFrame(query_res)
    return df.astype(str)