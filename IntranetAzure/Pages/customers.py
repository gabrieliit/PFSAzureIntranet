#import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash
from dash import Input, Output,State,callback_context
# Import dash_table with compatibility fallback
try:
    import dash_table
except ImportError:
    try:
        from dash import dash_table
    except ImportError:
        # Create a mock dash_table module if both imports fail
        import sys
        from types import ModuleType
        dash_table = ModuleType('dash_table')
        # Add basic DataTable class
        class DataTable:
            def __init__(self, *args, **kwargs):
                raise ImportError("dash_table.DataTable not available - please install dash-table package")
        dash_table.DataTable = DataTable
        sys.modules['dash_table'] = dash_table
from datetime import datetime,timedelta
import os
import json
import requests
import pandas as pd
from bson import json_util
#import required project modules
from Pages import user_auth_flow as uf,styles,shared_components as sc
from CommonDataServices import data_extractor as de
from Pages.Forms import customers_forms
#draw the customerspage
def draw_page_content(ext=[]):
    customers_content=html.Div(
        [
            # Display the Sample DataTable from Mongo Db
            html.Div(
                [
                    html.H1("View Customer details"),
                    html.H3("Enter search criteria"),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Name"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter customer name",id="customers-search-cust-input-cust-name"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Phone"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter Phone No",id="customers-search-cust-input-cust-phone"),width=3),
                        ]
                    ),
                    dbc.Button("Find customers",id="customers-btn-get-customer-data"),
                    html.Div(children=None, id="customers-div-customer-details"),
                    html.Hr()
                ],
                style={"margin-bottom":"75px"}, 
            ),
            html.H2("Manage Customers"),
            dbc.Row(
                [
                    dbc.Col(dbc.Button("Create Customer",id="customers-btn-create-customer",disabled=True)),
                    dbc.Col(dbc.Button("Update Customer",id="customers-btn-update-customer",disabled=True)),
                    dbc.Col(dbc.Button("Delete Customer",id="customers-btn-delete-customer",disabled=True)),
                ],
            ),
            html.Div(children=None, id="customers-div-manage-cust-forms"),
        ],
        style=styles.CONTENT_STYLE
    )   
    return customers_content

#Define an register callbacks
def register_callbacks(app):
    @app.callback(
            Output("customers-div-customer-details","children"),
            Input("customers-btn-get-customer-data","n_clicks"),
            State("customers-search-cust-input-cust-name","value"),
            State("customers-search-cust-input-cust-phone","value"),
    )
    def get_customer_data(n_clicks_get,cust_name,cust_phone):
        #set mongo db conn details
        attribs=["CustName","CustPhone"]
        values=[cust_name,cust_phone]
        filters={}
        for attrib,value in zip(attribs,values):
            if value:
                filters[attrib]=value
        if n_clicks_get:
            source_obj=de.source_factory("Customers",dataset_type="Consumer",db_name="PFS_MI")
            source_obj.load_data(filters)
            # Fetch data from MongoDB and convert to DataFrame
            children=sc.df_to_dash_table(source_obj.data,'customers-tbl-sample-mflix-comments')
        else:
            children= dash.no_update
        return children

    @app.callback(
            Output("customers-div-manage-cust-forms","children"),
            Input("customers-btn-update-customer","n_clicks"),
            Input("customers-btn-create-customer","n_clicks"),
            Input("customers-btn-delete-customer","n_clicks"),
    )
    def manage_customer_data(n_clicks_update, n_clicks_create,n_clicks_delete):
        trigger = callback_context.triggered[0]
        button_id = trigger["prop_id"].split(".")[0]
        if button_id=="customers-btn-create-customer":
            return customers_forms.draw_create_form()
        elif button_id=="customers-btn-update-customer":
            return customers_forms.draw_update_form()
        elif button_id=="customers-btn-delete-customer":
            return customers_forms.draw_delete_form()
        else:
            return dash.no_update

