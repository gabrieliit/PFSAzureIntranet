#import python packages
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash
from dash import Input, Output,State,callback_context
import pandas as pd
import plotly.express as px
#import required project modules
from Pages import styles,shared_components as sc
from CommonDataServices import data_extractor as de
from Pages.Forms import transactions_forms
#draw the transactionspage
REPORT_PARAMS={
    "DailyNetCashOutflows":{
        "StartDate":{"Placeholder":"dd-mm-yyyy"},
        "EndDate":{"Placeholder":"dd-mm-yyyy"},
        #"DeTrend":{"Value":"N"}
    }
}

def draw_page_content(ext=[]):
    reports_list=["DailyNetCashOutflows"]
    transactions_content=html.Div(
        [
            # Display the Sample DataTable from Mongo Db
            html.Div(
                [
                    html.H1("View Transaction details",style={'color':'blue'}),
                    html.H3("Enter search criteria"),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("GL No"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter GL No",id="transactions-search-txn-input-gl-no"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Receipt No"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter receipt no",id="transactions-search-txn-input-rec-no"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Date"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter transaction date",id="transactions-search-txn-input-rec-date"),width=3),
                        ]
                    ),
                    dbc.Button("Find transactions",id="transactions-btn-get-transaction-data"),
                    html.Div(children=None, id="transactions-div-transaction-details"),
                    html.Hr()
                ],
                style={"margin-bottom":"75px"}, 
            ),
            html.H2("Manage Transactions",style={'color':'blue'}),
            dbc.Row(
                [
                    dbc.Col(dbc.Button("Create Transaction",id="transactions-btn-create-transaction",disabled=True)),
                    dbc.Col(dbc.Button("Update Transaction",id="transactions-btn-update-transaction",disabled=True)),
                    dbc.Col(dbc.Button("Delete Transaction",id="transactions-btn-delete-transaction",disabled=True)),
                ],
            ),
            html.Div(children=None, id="transactions-div-manage-txn-forms"),
            html.Hr(),
            html.Div(
                dbc.Row(
                    [
                        html.H2("Custom Reports",style={'color':'blue'}),
                        dbc.Col(html.H6("Select Report",style={'display': 'flex', 'justify-content': 'flex-end'}),width=1),
                        dbc.Col(dcc.Dropdown(id="transactions-dd-sel-rep",options=reports_list),width=3), 
                    ]  
                )  
            ),
            html.Div(
                [
                    dbc.Row(
                        [
                            html.H4("Populate report params",style={'color':'blue'}),
                            dbc.Col(html.H6(id="transactions-lbl-param1",children="Param1",style={'display': 'flex', 'justify-content': 'flex-end','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param1"),width=1,style={'visibility':'hidden'},className='custom-ph',), 
                            dbc.Col(html.H6(id="transactions-lbl-param2",children="Param2",style={'display': 'flex', 'justify-content': 'flex-end','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param2"),width=1,style={'visibility':'hidden'},className='custom-ph'),
                            dbc.Col(html.H6(id="transactions-lbl-param3",children="Param3",style={'display': 'flex', 'justify-content': 'flex-end','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param3"),width=1,style={'visibility':'hidden'}), 
                        ],
                        style={'display': 'flex', 'align-items': 'center'} 
                    ),
                    dbc.Row(dbc.Col(dbc.Button("Generate Report",id="transactions-btn-gen-report"),width=2)),
                    dbc.Alert(id="transactions-alert-rep-gen-status-msg",is_open=False,color='info') 
                ]
            ),
            html.Div(
                id="transactions-div-display-report"
            )
        ],
        style=styles.CONTENT_STYLE
    )   
    return transactions_content

#Define an register callbacks
def register_callbacks(app):
    @app.callback(
            Output("transactions-div-transaction-details","children"),
            Input("transactions-btn-get-transaction-data","n_clicks"),
            State("transactions-search-txn-input-gl-no","value"),
            State("transactions-search-txn-input-rec-no","value"),
            State("transactions-search-txn-input-rec-date","value"),
    )
    def get_transaction_data(n_clicks_get,gl_no, rec_no,date):
        #set mongo db conn details
        attribs=["GLNo","RecNo.","Date"]
        values=[gl_no,rec_no,date]
        filters={}
        for attrib,value in zip(attribs,values):
            if value:
                filters[attrib]=value
        if n_clicks_get:
            source_obj=de.source_factory("Transactions",dataset_type="Consumer",db_name="PFS_MI")
            source_obj.load_data(filters)
            # Fetch data from MongoDB and convert to DataFrame
            children=sc.df_to_dash_table(source_obj.data,'transactions-tbl-sample-mflix-comments')
        else:
            children= dash.no_update
        return children

    @app.callback(
            Output("transactions-div-manage-txn-forms","children"),
            Input("transactions-btn-update-transaction","n_clicks"),
            Input("transactions-btn-create-transaction","n_clicks"),
            Input("transactions-btn-delete-transaction","n_clicks"),
    )
    def manage_transaction_data(n_clicks_update, n_clicks_create,n_clicks_delete):
        trigger = callback_context.triggered[0]
        button_id = trigger["prop_id"].split(".")[0]
        if button_id=="transactions-btn-create-transaction":
            return transactions_forms.draw_create_form()
        elif button_id=="transactions-btn-update-transaction":
            return transactions_forms.draw_update_form()
        elif button_id=="transactions-btn-delete-transaction":
            return transactions_forms.draw_delete_form()
        else:
            return dash.no_update



