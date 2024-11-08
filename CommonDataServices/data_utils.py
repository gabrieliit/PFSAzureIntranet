import pandas as pd

def mdb_query_postproc(query_res,dtype='native'):
    #at the moment this just converts the query results to string for display in dash tables
    #query results is output of a call to mongodb collection.find()
    df=pd.DataFrame(query_res)
    if dtype=='str':
        df=df.astype(str)
    return df

def cast_dfcol_to_type(df_col,dtype):
    if type(dtype)==str:#currently only date,intstr intstr type objects are specified as strings in the ColTypes metadata
        if dtype.startswith("date"):
            str_format=dtype[dtype.find("date-")+len("date-"):]#date dtype is specified as date-formatstring in ColTypes metadate
            df_col=pd.to_datetime(df_col, format=str_format,errors='coerce')
            df_col=df_col.fillna(pd.to_datetime('1900-01-01'))#fill na with default date
#           df_col=df_col.dt.strftime(str_format)#convert to standard string
        elif dtype=="intstr":#first convert to int and then cast to string to remove all decimals
            df_col=df_col.astype(int).astype(str)
    else:
        df_col=df_col.astype(dtype)
    return df_col
