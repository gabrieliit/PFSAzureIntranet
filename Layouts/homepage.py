#import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash
from dash import Input, Output,State,callback_context
import dash_table
from datetime import datetime,timedelta
import os
import json
import requests
import pandas as pd
from bson import json_util
#import required project modules
from Layouts import user_auth_flow as uf,styles
from DataServices import mongo_store as mdb, data_utils as du
from Layouts.Forms import accounts_forms
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
#draw the homepage
def draw_page_content(ext=[]):
    #define hompage content
    #get gold price
    try:
        with open("price.json", "r") as file:
            last_price_point=json.load(file)
            price=last_price_point["price"]
            price="{:.2f}".format(float(price))#format as 2 decimal
            source=last_price_point["source"]
            date=last_price_point["date"]
    except Exception as e:
        price="NA"
        source="NA"
        date="NA"
    #df=df.iloc[:,1:]

    homepage_content=html.Div(
        [
            html.Div(
            [
                html.Label("Last fetched gold price :", style={"margin-right":"10px"}),
                dcc.Input(value=f"{price}",style={"margin-right":"10px"},id="home-tb-price-XAU-display",readOnly=True),
                html.Label(f"Sourced from : {source} on {date}",id="home-lbl-price-XAU-source"),
                dbc.Button("Fetch Latest Gold Price", id="home-btn-fetch-xau-latest"),
                html.Div(id="home-div-price-XAU-display"),
                dcc.ConfirmDialog(
                    id="home-confirm-dialog-xau-price-persist",
                    message="Do you want to save the data to a local file?"
                ),
                dcc.Store(id="home-store-fetched-xau-price-latest"),
                html.Hr(),
            ],
            style={"margin-bottom":"75px"}), 
            # Display the Sample DataTable from Mongo Db
            html.Div(
                [
                    html.H1("View Account details"),
                    html.H3("Enter search criteria"),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("GL No"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter GL No",id="home-search-acc-input-gl-no"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Name"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter phone No",id="home-search-acc-input-cust-name"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Phone"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter Phone No",id="home-search-acc-input-cust-phone"),width=3),
                        ]
                    ),
                    dbc.Button("Find accounts",id="home-btn-get-account-data"),
                    html.Div(children=None, id="home-div-account-details"),
                    html.Hr()
                ],
                style={"margin-bottom":"75px"}, 
            ),
            html.H2("Manage Accounts"),
            dbc.Row(
                [
                    dbc.Col(dbc.Button("Create Account",id="home-btn-create-account")),
                    dbc.Col(dbc.Button("Update Account",id="home-btn-update-account")),
                    dbc.Col(dbc.Button("Delete Account",id="home-btn-delete-account")),
                ],
            ),
            html.Div(children=None, id="home-div-manage-acc-forms"),
        ],
        style=styles.CONTENT_STYLE
    )   
    return homepage_content

#Define an register callbacks
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
            Output("home-div-account-details","children"),
            Input("home-btn-get-account-data","n_clicks"),
            State("home-search-acc-input-gl-no","value"),
            State("home-search-acc-input-cust-name","value"),
            State("home-search-acc-input-cust-phone","value"),
    )
    def get_account_data(n_clicks_get,gl_no, cust_name,cust_phone):
        #set mongo db conn details
        db = mdb.mongo_client["PFS_MI"]
        collection = db["Accounts"]
        attribs=["GL_No","Customer_Phone","Customer_Name"]
        values=[gl_no,cust_name,cust_phone]
        filters={}
        for attrib,value in zip(attribs,values):
            if value:
                filters[attrib]=value
        if n_clicks_get:
            # Fetch data from MongoDB and convert to DataFrame
            query_results = collection.find(filters) 
            df = du.mdb_query_postproc(query_results)
            children=dash_table.DataTable(
                id='home-tbl-sample-mflix-comments',
                columns=[{"name": col, "id": col} for col in df.columns],
                data=df.to_dict('records'),
                # Other DataTable properties (e.g., pagination, sorting) can be customized here
                style_table=
                {
                    'overflowY': 'scroll',  # Enable vertical scrolling
                    'overflowX': 'scroll',
                    'maxHeight': '300px',  # Set the maximum height
                    'width': '100%',
                }
            )
        else:
            children= dash.no_update
        return children

    @app.callback(
            Output("home-div-manage-acc-forms","children"),
            Input("home-btn-update-account","n_clicks"),
            Input("home-btn-create-account","n_clicks"),
            Input("home-btn-delete-account","n_clicks"),
    )
    def manage_account_data(n_clicks_update, n_clicks_create,n_clicks_delete):
        trigger = callback_context.triggered[0]
        button_id = trigger["prop_id"].split(".")[0]
        if button_id=="home-btn-create-account":
            return accounts_forms.draw_create_form()
        elif button_id=="home-btn-update-account":
            return accounts_forms.draw_update_form()
        elif button_id=="home-btn-delete-account":
            return accounts_forms.draw_delete_form()
        else:
            return dash.no_update
"""
        elif n_clicks_create:
            # Define your document (record) as a Python dictionary
            new_document = {
                "name": "John Test",
                "text": "123 Main St",
                "email": "johntest123@testmail.com",
            }
            # Insert the document into the collection
            result = collection.insert_one(new_document)

            # Get the inserted document's ID
            inserted_id = result.inserted_id
            print(f"Inserted document ID: {inserted_id}")
        elif n_clicks_update:
            query_filter={"name":"Mercedes Tyler"}
            update_operation={{"$set"}:{"name":"Big Momma"}}
            # Update the document
            result = collection.update_many(query_filter, update_operation)
            children=html.Label(f"Modified {result.modified_count} document(s)")
"""
