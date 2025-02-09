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
from ConsumerServices.DatasetTools import consumer_sources_metadata as csm
from ConsumerServices.DatasetTools.DatasetDefs import gold_prices
from Pages.ManageData import gold_prices_forms
from Pages import styles

# Define constants
METAL_PRICE_API_KEY=os.environ["METAL_PRICE_API_KEY"]
METAL_PRICE_API_LATEST=f"https://api.metalpriceapi.com/v1/latest?api_key={METAL_PRICE_API_KEY}&base=XAU&currencies=INR"
GM_PER_TROY_OUNCE=31.103

#draw the homepage
def draw_page_content(ext=[],pd_refact={}):
    #define hompage content
    #get gold price
    try:
        results=gold_prices.get_gold_price()
        latest_price_data = max(results['Results'], key=lambda x: x['COBDate'])
        price='{:.2f}'.format(float(latest_price_data['Value']))
        price_source = latest_price_data['Source']
        date = latest_price_data['COBDate'].strftime('%d-%b-%Y')
    except Exception as e:
        price="NA"
        price_source="NA"
        date="NA"
    manage_ref_data_content=html.Div(
        [
            html.Div(
                [
                    html.Label("Latest gold price in datastore :", style={"margin-right":"10px"}),
                    dcc.Input(value=f"{price}",style={"margin-right":"10px"},id="home-tb-price-XAU-display",readOnly=True),
                    html.Label(f"Sourced from {price_source} for {date}",id="home-lbl-price-XAU-source"),
                    html.Hr(),
                ],
                style={"margin-bottom":"75px"}
            ),
            html.Div(
                [                
                    html.H5("Fetch Gold Price",style={'color':'blue'}),
                    dbc.Row(
                        [
                            dbc.Col(html.H6("Pick/Enter COB (dd-mm-yyyy)",style={'display': 'flex', 'justify-content': 'center', 'align-items': 'center'},),width=3),
                            dbc.Col(
                                dcc.DatePickerSingle(
                                    id='refdata-dp-cob-date',
                                    display_format='DD-MM-YYYY',
                                    style={"width":"200px"}
                                ),
                                width=2,
                            ),
                            dbc.Col(dbc.Button("Fetch Gold Price", id="refdata-btn-fetch-xau-price"),width=2),
                            dbc.Col(dbc.Button("Create Manual Record", id="refdata-btn-create-manual-record"),width=2),    
                        ],
                    ),
                    html.Div(id="refdata-div-price-XAU-display"),
                    dbc.Alert(id="home-alert-dash-prep-status",is_open=False,color='info'),
                    html.Div(id="home-div-summary-dash")
                ],
                style={'height': '100vh','width':'80vw', 'overflow': 'scroll'}        
            )
        ],
        style=styles.CONTENT_STYLE        
    )   
    return manage_ref_data_content

def register_callbacks(app):
    @app.callback(
        Output("refdata-div-price-XAU-display", "children"),
        Input("refdata-btn-fetch-xau-price", "n_clicks"),
        Input("refdata-btn-create-manual-record", "n_clicks"),
        State('refdata-dp-cob-date','date'),
        prevent_initial_call=True
    )
    def fetch_and_confirm(n_clicks, n_clicks_manual,cob_date):
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if triggered_id == "refdata-btn-fetch-xau-price":
            # get the gold price for cob date
            result=gold_prices.get_gold_price(pd.to_datetime(cob_date))
            if result["APICall"]:#if price was not available in MDB, would have trigerred a call to API
                if result["APIResponse"]["Status"]=="Success":
                    price=result["Results"][0]
                    msg=f"Price not availabe in datastore. Fetched from MetalPrice.com API - {price} and persisted to datastore."
                    alert_color="warning"
                else:
                    price={}#store empty dict
                    msg=f'Price not available in datastore and couldnt be fetched MetalPrice.com API due to - {result["APIResponse"]["Message"]}.\nTry creating record manually.'
                    alert_color="danger"
            else: #price was available in MDB.Display it
                price=result["Results"][0]
                msg=f"Price fetched from datastore - {price}"
                alert_color="info"
            children=dbc.Alert(msg,color=alert_color)
        elif triggered_id == "refdata-btn-create-manual-record":
            children=gold_prices_forms.draw_create_form(cob_date)
        return children

