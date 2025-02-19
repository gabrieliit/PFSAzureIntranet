import os
import requests
from datetime import datetime
#import modules
from CommonDataServices import data_extractor as de
def get_gold_price(cob_date=None):
    #check if COB data is available in MongoDB
    source_obj=de.source_factory("GoldPrices",dataset_type="Consumer")
    if not cob_date:#return all gold prices in MD
        query_filter={}
    else:
        query_filter={"COBDate":cob_date,"SourceType":"CSV"}
    #first check mongodb has price data from PendingLoan_30 files
    gold_price_doc=source_obj.find(filter=query_filter,ret_type='docs')
    results={
            "Results":gold_price_doc["Results"],
            "MDBQueryFilter":{"COBDate":cob_date,"SourceType":"CSV"},
            "APICall":False,
            "APIResponse":None,
            "WritetoMDB":False,
            "WriteResult":None
        } 
    if len(gold_price_doc["Results"])==0:
        #if not found, check if COB data has already been sourced from MetalPrices API
        query_filter["SourceType"]="API"
        gold_price_doc=source_obj.find(filter=query_filter,ret_type='docs')
    if len(gold_price_doc["Results"])==0:
        #if not found, check if COB data has already been stored manually
        query_filter["SourceType"]="Manual Entry"
        gold_price_doc=source_obj.find(filter=query_filter,ret_type='docs')
    if len(gold_price_doc["Results"])==0:
        #if API data not available in MDB, source from MetalPrices API and write into MDB
        source_obj=de.source_factory("MetalPricesAPI",dataset_type="Consumer")
        api_response=source_obj.find(filter={"COBDate":cob_date,"Base":"INR","CCYList":["XAU"]},ret_type='docs')
        #format results into a doc matching the schema of the GoldPrices collection
        gold_price_doc={"COBDate":cob_date,"SourceType":"API","Source":"MetalPricesAPI","Value":api_response["Results"]}
        if api_response["Status"]=="Success":
            #store the results in MongoDB
            source_obj=de.source_factory("GoldPrices",dataset_type="Consumer")
            write_result=source_obj.insert(gold_price_doc)
        else:
            write_result={"Status":"Errors","InsertedID":None,"WriteErrors":api_response["Message"]}
        results={
            "Results":[gold_price_doc],
            "MDBQueryFilter":None,
            "APICall":True,
            "APIResponse":api_response,
            "WritetoMDB":True,
            "WriteResult":write_result
        }        
    else:#query was successful in locating API data for COB in MDB
        results={
            "Results":gold_price_doc["Results"],
            "MDBQueryFilter":{"COBDate":cob_date,"SourceType":"API"},
            "APICall":False,
            "APIResponse":None,
            "WritetoMDB":False,
            "WriteResult":None
        } 
    return results
    
def set_gold_price(price,cob,source,source_type,username):
    source_obj=de.source_factory("GoldPrices",dataset_type="Consumer")
    gold_price_doc={
            "COBDate":cob,
            "SourceType":source_type,
            "Source":source,
            "Value":price,
            "UserName":username
    }
    write_result=source_obj.insert(gold_price_doc)
    return write_result
 