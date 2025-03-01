import pandas as pd
import random
from flask import session
from datetime import datetime as dt

NotingTypeMap={
    "Princ. Rec.":"PrincRec",
    "Principal Rec.":"PrincRec",
    "Interest Due":"IntDue",
    "Princi. Due":"PrincDue",
    "Principal Balance":"PrincDue",
    "Principal Bal.":"PrincDue",
    "Interest Rate":"IntRate",
    "Int. Rate":"IntRate",
    "Interest Received":"IntRec",
    "Int. Rec.":"IntRec",
    "Interest Outstanding":"IntDue",
    "Int. Bal":"IntDue",
    "Total Outstanding":"TotalDue",
    "Average Val. per Gram":"PrincPerGm",
    "Market Value":"CollVal",
    "Difference":"MarginVal",
    "Received Upto":"RecvdUpto",
    "Pending Days":"PendingDays",
    "Notice Type":{"Notice 1":"Notice1","Notice 2":"Notice2","Auction":"Auction","Reg.Letter":"RegLet"}
}

TxnTypeMap={
    "Prici. Rec":"PrincPayment",
    "Int. Rec.":"IntPayment"
}

NotingDateMap={
    'CollVal':{"Val":"COBDate","ValType":"Constant"},
    'PrincPerGm':{"Val":"COBDate","ValType":"Constant"},
    'MarginVal':{"Val":"COBDate","ValType":"Constant"},
    'PrincDue':{"Val":"COBDate","ValType":"Constant"},
    'IntDue':{"Val":"COBDate","ValType":"Constant"},
    'PrincRec':{"Val":"COBDate","ValType":"Constant"},
    'IntRec':{"Val":"COBDate","ValType":"Constant"},
    'TotalDue':{"Val":"COBDate","ValType":"Constant"},
    'Notice1':{"Val":"Notice Date","ValType":"SourceAttrib"},
    'Notice2':{"Val":"Notice Date","ValType":"SourceAttrib"},
    'Auction':{"Val":"Notice Date","ValType":"SourceAttrib"},
    'RegLet':{"Val":"Notice Date","ValType":"SourceAttrib"}, 
    'PendingDays':{"Val":"COBDate","ValType":"Constant"},
    'RecvdUpto':{"Val":"COBDate","ValType":"Constant"},
    'IntRate':{"Val":"COBDate","ValType":"Constant"}
}

def get_constants(data_transformer, const):
    #returns constants. Casts to cast_type
    if const=="COBDate":
        result=pd.to_datetime(data_transformer.cob_date)
    elif const=="UserName":
        result=session["user"]["name"]
    elif const=="TimeNow":
        result=dt.now()
    elif const=="FileName":
        result=data_transformer.source_filename
    return result

def cast_to_type(val, cast_type):
    #casts val to type cast_type
    if cast_type==str:
        result=str(val)
    elif cast_type=="date":
        result=pd.to_datetime(val)
    return result

def evaluate_condition(row,record,row_condition,attr_condition):
    #evaluates if condition is met by the row 
    if row_condition["Op"]=="==":
        row_match=True if row[row_condition["CondCol"]]==row_condition["CondVal"] else False
    #evaluate if condition is met by col
    if attr_condition["Op"]=="NotIn":
        col_match=True if record[attr_condition["CondAttr"]] not in attr_condition["CondVal"] else False
    return True if (row_match and col_match) else False

"""def dedup(tgt_collection,docs):
    #dedup by updating a column value where duplicates exists
    df=pd.DataFrame(docs)
    filters=tm.TargetSchemas[schema]["dedup"]["filter_cols"]
    col=tm.TargetSchemas[schema]["dedup"]["update_cols"]
    dup_rows=df.duplicated(subset=filters,keep="first")
    #update the update column randomly by +/- delta amount
    delta=tm.TargetSchemas[schema]["dedup"]["delta_amt"]
    df.loc[dup_rows,col]=df.loc[dup_rows,col].apply(lambda x: x + delta*random.choice([1,-1]))
    return df.to_dict(orient='records')"""
