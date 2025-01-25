#import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash
from dash import Input, Output,State,callback_context
from dateutil.relativedelta import relativedelta as rd
from datetime import datetime,timedelta, date as dt
import os
import json
import requests
import pandas as pd
import locale
#import required project modules
from Pages import user_auth_flow as uf,styles,shared_components as sc
from CommonDataServices import mongo_store as mdb, data_utils as du, data_extractor as de
from Pages.Forms import accounts_forms
import Pages.Home.home_pagedata as hpd
from ConsumerServices.DatasetTools import consumer_sources_metadata as csm
from ConsumerServices.DatasetTools.DatasetDefs import ref_data

locale.setlocale(locale.LC_ALL,'en_US.utf8')
event_count=0
#Connection details
hist_period=7
end_date=datetime.today()
start_date=end_date-timedelta(days=hist_period)
end_date=end_date.strftime("%Y-%m-%d")
start_date=start_date.strftime("%Y-%m-%d")
METAL_PRICE_API_KEY=os.environ["METAL_PRICE_API_KEY"]
METAL_PRICE_API_LATEST=f"https://api.metalpriceapi.com/v1/latest?api_key={METAL_PRICE_API_KEY}&base=XAU&currencies=INR"
METAL_PRICE_API_HIST=f"https://api.metalpriceapi.com/v1/timeframe?api_key={METAL_PRICE_API_KEY}&start_date={start_date}&end_date={end_date}&base=XAU&currencies=INR"
GM_PER_TROY_OUNCE=31.103

def calc_pagedata(rep_cob):
    #get values to be displayed from mdb
    page_data={}
    page_data["YoY%"]={}
    page_data["QoQ%"]={}
    qoq_cob=rep_cob-rd(months=3)
    yoy_cob=rep_cob-rd(years=1)
    for source,agg_list in hpd.agg_pipe_outputs.items():
        for agg_rule in agg_list:
            pipe=agg_rule["PipeName"]
            try:
                params=agg_rule["PipeParams"]
            except KeyError:
                params=None
            source_obj=de.source_factory(source,dataset_type="Consumer")
            page_data[pipe]={}
            for cob in agg_rule["AsofDate"]:
                rep_cob_delta=cob["Delta"]
                as_of_date=rep_cob+rep_cob_delta
                #retrieve pipe params
                param_dict={}
                if params:
                    for param in params:
                        param_val=hpd.get_fact(param["ParamDef"],{"AsofDate":as_of_date},fact_coll=page_data)
                        param_name=param["ParamName"]
                        if type(param_val)!=dict:
                            param_dict.update({param_name:param_val})
                        else:
                            param_dict.update(param_val)
                else:
                    param_dict={}
                page_data[pipe][as_of_date.strftime("%d-%b-%Y")]={}
                agg_results=source_obj.aggregate(pipe,as_of_date,param_dict)
                for fact,fact_procs in agg_rule["Facts"].items():
                    col=fact_procs["OutputCol"]
                    if not fact_procs["Dim"]:
                        res=0.0 if agg_results.empty else agg_results[col][0]
                    else: #if fact has a dimension, read from the dimension row of the agg_Results df
                        res=0.0 if agg_results.empty else agg_results[agg_results['_id']==fact_procs["Dim"]][col].values[0]
                    for pp,rule in fact_procs["PostProcs"].items(): 
                        if pp=="FormatDateString": res=res.strftime(rule)
                    page_data[pipe][as_of_date.strftime("%d-%b-%Y")][fact]=res
    #ref data
    for fact_def in hpd.ref_data_facts:
        page_data["RefData"]={}
        for cob in fact_def["AsofDate"]:
            rep_cob_delta=cob["Delta"]
            as_of_date=rep_cob+rep_cob_delta
            if fact_def["Source"]=="GoldPrices":
                res=ref_data.get_gold_price(as_of_date)["Results"][0]["Value"]
            try:
                page_data["RefData"][as_of_date.strftime("%d-%b-%Y")][fact_def["FactName"]]=res
            except KeyError:
                page_data["RefData"][as_of_date.strftime("%d-%b-%Y")]={}#initiate the cob dict
                page_data["RefData"][as_of_date.strftime("%d-%b-%Y")][fact_def["FactName"]]=res
    #post procs
    for level, facts in hpd.post_procs.items():    
        for calc_def in facts:
            fact=calc_def["FactName"]
            if calc_def["CalcType"]=="SumFacts":
                cobs_list=[(rep_cob+cob["Delta"]).strftime("%d-%b-%Y") for cob in calc_def["AsofDate"]]
                for item in calc_def["PageFactsList"]:
                    pipe=item["PipeName"]
                    try:
                        mult=item["Multiplier"]
                    except KeyError:
                        mult=1.0
                    for cob in cobs_list:
                        try:#subsequent iterations of adding facts into sum
                            page_data[level][cob][fact]+=(mult*page_data[pipe][cob][item["PageDataItem"]])
                        except KeyError:#First fact in sum
                            try:
                                page_data[level][cob][fact]=page_data[pipe][cob][item["PageDataItem"]]
                            except KeyError:#First post proc result for cob. Intiate dict first 
                                try:
                                    page_data[level][cob]={}
                                except KeyError:#First post proc result for level. Initiate level dict first
                                    page_data[level]={}
                                    page_data[level][cob]={}
                                page_data[level][cob][fact]=page_data[pipe][cob][item["PageDataItem"]]
            elif calc_def["CalcType"]=="AvgOverTimeInterval":
                #read params from home_pagedata.py post calc_defs dict
                cobs_list=[(rep_cob+cob["Delta"]) for cob in calc_def["AsofDate"]]
                interval=calc_def["Interval"]
                daycount=calc_def["DayCount"]
                for item in calc_def["PageFactsList"]:
                    pipe=item["PipeName"]
                    for cob in cobs_list:
                        bus_days=du.calc_business_days(cob-interval,cob,daycount["Weekend"])
                        cob=cob.strftime("%d-%b-%Y")#convert to string for dict refs
                        try:#subsequent iterations of adding facts for cob
                            page_data[level][cob][fact]=page_data[pipe][cob][item["PageDataItem"]]/bus_days
                        except KeyError:
                            try:
                                page_data[level][cob]={} #initiate the cob dict
                            except KeyError:#first item in the level, Initiate the level dict and then the cob dict
                                page_data[level]={}
                                page_data[level][cob]={}
                            page_data[level][cob][fact]=page_data[pipe][cob][item["PageDataItem"]]/bus_days
            elif calc_def["CalcType"]=="FactArithmetic":
                #read params from home_pagedata.py post calc_defs dict
                cobs_list=[(rep_cob+cob["Delta"]) for cob in calc_def["AsofDate"]]
                for cob in cobs_list:
                    calc_val=0.0
                    for term in calc_def["Terms"]:
                        if term["Op"]=="Div":
                            num=hpd.get_fact(term["Num"],{"AsofDate":cob},fact_coll=page_data)
                            denom=hpd.get_fact(term["Denom"],{"AsofDate":cob},fact_coll=page_data)
                            term["Value"]=num/denom
                        calc_val+=term["Value"]
                    cob_str=cob.strftime("%d-%b-%Y")
                    try:#subsequent iterations of adding facts for cob
                        page_data[level][cob_str][fact]=calc_val
                    except KeyError:
                        try:
                            page_data[level][cob_str]={} #initiate the cob dict
                        except KeyError:#first item in the level, Initiate the level dict and then the cob dict
                            page_data[level]={}
                            page_data[level][cob_str]={}
                        page_data[level][cob_str][fact]=calc_val    
    #refactor page data dict to {FAct : {COB_Date:Value}} 
    pd_refact={}
    for pipe,pipe_res in page_data.items():
        for cob,facts in pipe_res.items():
            for fact,val in facts.items():
                try:
                    pd_refact[fact][cob]=val
                except KeyError:
                    pd_refact[fact]={} #intialise pd_refact[fact] dict
                    pd_refact[fact][cob]=val
    #calculate yoy and qoq movements
    for fact,vals in pd_refact.items():
        try:  
            vals["YoY%"]=f'{vals[rep_cob.strftime("%d-%b-%Y")]/vals[yoy_cob.strftime("%d-%b-%Y")]-1:.2%}'
        except ZeroDivisionError as e:
            vals["YoY%"]="NA"
        try:
            vals["QoQ%"]=f'{vals[rep_cob.strftime("%d-%b-%Y")]/vals[qoq_cob.strftime("%d-%b-%Y")]-1:.2%}'
        except ZeroDivisionError as e:
            vals["QoQ%"]="NA"
    return pd_refact

def draw_summary_dashboard(rep_cob):
    print("hi1")
    pd_refact=calc_pagedata(rep_cob)
    return [
        html.Div(
            [ 
                html.Div(
                    [
                        html.H3("Portfolio Metrics",style={'color':'blue'}),
                        dbc.Row(
                            [
                                dbc.Col(html.H5("Metric"),width=5),
                                dbc.Col(html.H5("Value"),width=3,className="text-end"),
                                dbc.Col(html.H5("YoY%"),width=2),
                                dbc.Col(html.H5("QoQ%"),width=2),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("No of Loans",width=5),
                                dbc.Col(f'{pd_refact["TotalAccounts"][rep_cob.strftime("%d-%b-%Y")]}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalAccounts"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalAccounts"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Principal o/s",width=5),
                                dbc.Col(f'{locale.format_string("%d",pd_refact["TotalPrincOutstanding"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOutstanding"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOutstanding"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Coll Weight",width=5),
                                dbc.Col(f'{locale.format_string("%d",pd_refact["TotalCollWeight"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalCollWeight"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalCollWeight"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Gold Price",width=5),
                                dbc.Col(f'{locale.format_string("%d",pd_refact["GoldPrice"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["GoldPrice"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["GoldPrice"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),                        
                        dbc.Row(
                            [
                                dbc.Col("New loans in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%d",pd_refact["NewLoansCount1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansCount1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansCount1Q"]["QoQ%"]}',width=2,className="text-end")                                
                            ]                                    
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Closed loans in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%d",pd_refact["LoanClosuresCount1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresCount1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresCount1Q"]["QoQ%"]}',width=2,className="text-end")                                
                            ]                                    
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Interest received in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["IntRec1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["IntRec1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["IntRec1Q"]["QoQ%"]}',width=2,className="text-end")  
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Principal received in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["PrincRec1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["PrincRec1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["PrincRec1Q"]["QoQ%"]}',width=2,className="text-end")  
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Net cash outflow in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["NetCashOutflows1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["NetCashOutflows1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["NetCashOutflows1Q"]["QoQ%"]}',width=2,className="text-end")  
                            ]
                        ),                             
                    ],
                    style={'border': '1px solid grey', 'padding': '10px','height': '100%', 'width':'50%','overflowY': 'auto'},
                ), 
                html.Div(
                    [
                        html.H3("Customers",style={'color':'blue'}),
                        dbc.Row(
                            [
                                dbc.Col(html.H5("Metric"),width=5),
                                dbc.Col(html.H5("Value"),width=3,className="text-end"),
                                dbc.Col(html.H5("YoY%"),width=2),
                                dbc.Col(html.H5("QoQ%"),width=2),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("No of Active Customers",width=6)
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("New Customers acquired in Qtr",width=6)
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("No of inactive customers",width=6)
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Avg no of loans per customer",width=6)
                            ]
                        ),          
                    ], 
                    style={'border': '1px solid grey', 'padding': '10px','height': '100%', 'width':'50%','overflowY': 'auto'}) 
            ], 
            style={'display': 'flex', 'height': '40vh', 'width':'64rem'}
        ), 
        html.Div(
            [ 
                html.Div(
                    [
                        html.H3("Transaction Metrics",style={'color':'blue'}),
                        dbc.Row(
                            [
                                dbc.Col(html.H5("Metric"),width=5),
                                dbc.Col(html.H5("Value"),width=3,className="text-end"),
                                dbc.Col(html.H5("YoY%"),width=2),
                                dbc.Col(html.H5("QoQ%"),width=2),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Avg new loan size in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["NewLoansAvgPrinc1QPL"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansAvgPrinc1QPL"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansAvgPrinc1QPL"]["QoQ%"]}',width=2,className="text-end")    
                            ],
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Avg cls loan size in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["LoanClosuresPrinc1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresPrinc1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresPrinc1Q"]["QoQ%"]}',width=2,className="text-end")    
                            ],
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Avg cls loan coll wt in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["LoanClosuresCollWeight1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresCollWeight1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["LoanClosuresCollWeight1Q"]["QoQ%"]}',width=2,className="text-end")    
                            ],
                        ),    
                        dbc.Row(
                            [
                                dbc.Col("Avg new coll wt in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["NewLoansCollWeight1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansCollWeight1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["NewLoansCollWeight1Q"]["QoQ%"]}',width=2,className="text-end")    
                            ],
                        ),
                        dbc.Row(
                            [
                                dbc.Col("No of receipts/day in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["AvgReceiptCountPerDay1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["AvgReceiptCountPerDay1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["AvgReceiptCountPerDay1Q"]["QoQ%"]}',width=2,className="text-end")  
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("New loans/day in Qtr",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["AvgNewLoansPerDay1Q"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["AvgNewLoansPerDay1Q"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["AvgNewLoansPerDay1Q"]["QoQ%"]}',width=2,className="text-end")  
                            ]
                        ),
                    ], 
                    style={'border': '1px solid grey', 'padding': '10px','height': '100%','width':'50%','overflowY': 'auto'}), 
                html.Div(
                    [
                        html.H3("Risk Metrics",style={'color':'blue'}),
                        dbc.Row(
                            [
                                dbc.Col(html.H5("Metric"),width=5),
                                dbc.Col(html.H5("Value"),width=3,className="text-end"),
                                dbc.Col(html.H5("YoY%"),width=2),
                                dbc.Col(html.H5("QoQ%"),width=2),
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Portfolio LTV",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["PortfolioLTV"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["PortfolioLTV"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["PortfolioLTV"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("No of loans LTV > 75% ",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["AccountCount_LTVBreaches"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_LTVBreaches"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_LTVBreaches"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Princ O/S LTV > 75% ",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["TotalPrincOutstanding_LTVBreaches"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOutstanding_LTVBreaches"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOutstanding_LTVBreaches"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col("Loans past due date",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["AccountCount_PastCls"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_PastCls"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_PastCls"]["QoQ%"]}',width=2,className="text-end")

                            ]
                        ), 
                        dbc.Row(
                            [
                                dbc.Col("Princ o/s past loan due date",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["TotalPrincOS_PastCls"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOS_PastCls"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOS_PastCls"]["QoQ%"]}',width=2,className="text-end")

                            ]
                        ), 
                        dbc.Row(
                            [
                                dbc.Col("Accounts with notices",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["AccountCount_Notices"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_Notices"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["AccountCount_Notices"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),                                   
                        dbc.Row(
                            [
                                dbc.Col("Princ OS - notices",width=5),
                                dbc.Col(f'{locale.format_string("%.2f",pd_refact["TotalPrincOS_Notices"][rep_cob.strftime("%d-%b-%Y")],grouping=True)}',width=3,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOS_Notices"]["YoY%"]}',width=2,className="text-end"),
                                dbc.Col(f'{pd_refact["TotalPrincOS_Notices"]["QoQ%"]}',width=2,className="text-end")
                            ]
                        ),
                    ], 
                    style={'border': '1px solid grey', 'padding': '10px','height': '100%','width':'50%','overflowY': 'auto'}) 
            ], 
            style={'display': 'flex', 'height': '40vh','width':'64rem'}
        )
    ]
#draw the homepage
def draw_page_content(ext=[],pd_refact={}):
    #define hompage content
    #get gold price
    try:
        with open("price.json", "r") as file:
            last_price_point=json.load(file)
            price=last_price_point["price"]
            price="{:.2f}".format(float(price))#format as 2 decimal
            price_source=last_price_point["source"]
            date=last_price_point["date"]
    except Exception as e:
        price="NA"
        source="NA"
        date="NA"
    #calculate latest reporting COB
    source_obj=de.source_factory("LoadJobs",dataset_type="Consumer")
    agg_results=source_obj.aggregate("COBDateList")
    cobs_loaded=agg_results["COBDateList"][0]
    cobs_loaded.sort(reverse=True)
    latest_cob=max(cobs_loaded)
    cob_labels=[cob.strftime("%d-%b-%Y") for cob in cobs_loaded]
    dd_options=[{"value":cob,"label":label} for cob,label in zip(cobs_loaded,cob_labels)]
    #df=df.iloc[:,1:]
    #cob=page_data["LatestCOBDate"]#aggregate returns a single row df. Extract the date
    #acc_count=locale.format_string("%d",page_data["TotalAccounts"]["AccountCount"][0],grouping=True)
    homepage_content=html.Div(
        [
            html.Div(
                [
                    html.Label("Last fetched gold price :", style={"margin-right":"10px"}),
                    dcc.Input(value=f"{price}",style={"margin-right":"10px"},id="home-tb-price-XAU-display",readOnly=True),
                    html.Label(f"Sourced from : {price_source} on {date}",id="home-lbl-price-XAU-source"),
                    dbc.Button("Fetch Latest Gold Price", id="home-btn-fetch-xau-latest"),
                    html.Div(id="home-div-price-XAU-display"),
                    dcc.ConfirmDialog(
                        id="home-confirm-dialog-xau-price-persist",
                        message="Do you want to save the data to a local file?"
                    ),
                    dcc.Store(id="home-store-fetched-xau-price-latest"),
                    html.Hr(),
                ],
                style={"margin-bottom":"75px"}
            ),
            html.Div(
                [                
                    dbc.Row(
                        [
                            dbc.Col(html.H6(id="home-h5-dashboard-cob",children=f"Select COB : ",style={'color':'blue'}),width=3),
                            dbc.Col(html.H6("Pick/Enter custom COB (dd-mm-yyyy)",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue'},),width=3),
                            dbc.Col(
                                dcc.DatePickerSingle(
                                    id='home-dp-custom-cob-date',
                                    display_format='DD-MM-YYYY',
                                    style={"width":"200px"}
                                ),
                                width=1,
                            ),
                            dbc.Col(html.H6("Select reporting COB",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue'}),width=2),
                            dbc.Col(dcc.Dropdown(id="home-dd-sel-rep-cob",options=dd_options),width=2),                      
                        ]
                    ),
                    dbc.Alert(id="home-alert-dash-prep-status",is_open=False,color='info'),
                    html.Div(id="home-div-summary-dash")
                ],
                style={'height': '100vh','width':'80vw', 'overflow': 'scroll'}        
            )
        ],
        style=styles.CONTENT_STYLE        
    )   
    return homepage_content

#Define and register callbacks
def register_callbacks(app):
    @app.callback(
        Output("home-store-fetched-xau-price-latest", "data"),
        Output("home-confirm-dialog-xau-price-persist", "displayed"),
        Output("home-tb-price-XAU-display", "value"),
        Output("home-lbl-price-XAU-source", "children"),
        Input("home-btn-fetch-xau-latest", "n_clicks"),
        Input("home-confirm-dialog-xau-price-persist", "submit_n_clicks"),
        State("home-store-fetched-xau-price-latest", "data"),
        prevent_initial_call=True
    )
    def fetch_and_confirm(n_clicks, submit_n_clicks,last_fetched_price):
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if triggered_id == "home-btn-fetch-xau-latest":
            # Replace with your API URL
            url = METAL_PRICE_API_LATEST
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                price_per_gm=data["rates"]["INR"]/GM_PER_TROY_OUNCE
                date=datetime.today().strftime("%Y-%m-%d")
                price_point={"price":price_per_gm,"date":date,"source" : METAL_PRICE_API_LATEST,}
                return price_point, True, "{:.2f}".format(float(price_per_gm)),f"Sourced from : {METAL_PRICE_API_LATEST} on {date}"
            else:
                try:
                    with open("price.json", "r") as file:
                        last_price_point=json.load(file)
                        price=last_price_point["price"]
                        source=last_price_point["source"]
                        date=last_price_point["date"]
                    return dash.no_update, dash.no_update, dash.no_update,f"Failed to fetch data. Last sourced from : {source} on {date}"
                except:
                    return dash.no_update, dash.no_update, dash.no_update,"Failed to fetch data"

        elif triggered_id == "home-confirm-dialog-xau-price-persist":
            if submit_n_clicks:
                price=last_fetched_price["price"]
                date=last_fetched_price["date"]
                # Save data to local file
                with open("price.json", "w") as f:
                    json.dump(last_fetched_price, f)
                return dash.no_update, False, "{:.2f}".format(float(price)), f"Sourced from {METAL_PRICE_API_LATEST} on {date}"
            return dash.no_update, False, dash.no_update

    @app.callback(
        Output("home-alert-dash-prep-status", "children"),
        Output("home-alert-dash-prep-status", "is_open"),
        Output("home-h5-dashboard-cob","children"),
        Input("home-dd-sel-rep-cob", "value"),
        Input('home-dp-custom-cob-date',"date"),
    )
    def read_rep_cob(rep_cob,cust_cob):
        ctx = callback_context
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]
        if triggered_id=='':
            raise dash.exceptions.PreventUpdate
        elif triggered_id=="home-dd-sel-rep-cob" or triggered_id=='':
            sel_cob=pd.to_datetime(rep_cob)
            alert_msg="Summary dashboard population in progress"
            h5_str=f"Selected COB : {sel_cob.strftime('%d-%b-%Y')}"
        elif triggered_id=='home-dp-custom-cob-date':
            #sel_cob=pd.to_datetime(cust_cob)
            h5_str="Selcted COB : None"
            alert_msg="Custom date selection is currently disabled. Select reporting COB."
        return alert_msg,True,h5_str

    @app.callback(
        Output("home-div-summary-dash", "children"),
        Output("home-alert-dash-prep-status", "is_open",allow_duplicate=True),
        Input("home-h5-dashboard-cob","children"),
        prevent_initial_call='initial_duplicate'
    )
    def generate_summary_dashboard(sel_cob_msg):
        #extract cob from "Selected COB : {sel_cob.strftime('%d-%b-%Y')}"
        cob=sel_cob_msg.split(": ")[1]
        if cob=='None': 
            raise dash.exceptions.PreventUpdate
        cob = pd.to_datetime(cob, format='%d-%b-%Y')
        return draw_summary_dashboard(cob), False
