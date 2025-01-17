#import python packages
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash
from dash import Input, Output,State,callback_context
#import required project modules
from Pages import styles,shared_components as sc
from CommonDataServices import data_extractor as de
from Pages.Forms import transactions_forms
#draw the transactionspage
REPORT_PARAMS={
    "DailyNetCashOutflows":{
        "StartDate":{"Placeholder":"dd-mm-yyyy"},
        "EndDate":{"Placeholder":"dd-mm-yyyy"},
        "DeTrend":{"Value":"N"}
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
                        dbc.Col(html.H6("Select Report",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue'}),width=1),
                        dbc.Col(dcc.Dropdown(id="transactions-dd-sel-rep",options=reports_list),width=3), 
                    ]  
                )  
            ),
            html.Div(
                [
                    html.H2("Custom Reports",style={'color':'blue'}),
                    dbc.Row(
                        [
                            dbc.Col(html.H6(id="transactions-lbl-param1",children="Param1",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param1"),width=1,style={'visibility':'hidden'}), 
                            dbc.Col(html.H6(id="transactions-lbl-param2",children="Param2",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param2"),width=1,style={'visibility':'hidden'}),
                            dbc.Col(html.H6(id="transactions-lbl-param3",children="Param3",style={'display': 'flex', 'justify-content': 'flex-end','color':'blue','visibility':'hidden'}),width=1),
                            dbc.Col(dbc.Input(id="transactions-ip-param3"),width=1,style={'visibility':'hidden'}), 
                        ]  
                    )  
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

    @app.callback(
        Output("transactions-lbl-param1","children"),
        Output("transactions-lbl-param1","style"),
        Output("transactions-ip-param1","style"),
        Output("transactions-ip-param1","value"),
        Output("transactions-lbl-param2","children"),
        Output("transactions-lbl-param2","style"),
        Output("transactions-ip-param2","style"),
        Output("transactions-ip-param2","placeholder"),
        Output("transactions-ip-param2","value"),
        Output("transactions-lbl-param3","children"),
        Output("transactions-lbl-param3","style"),
        Output("transactions-ip-param3","style"),
        Output("transactions-ip-param3","placeholder"),
        Output("transactions-ip-param3","value"),        
        Output("transactions-div-display-report","children"),
        Input("transactions-dd-sel-rep",'value'),
        State("transactions-lbl-param1","style"),
        State("transactions-ip-param1","style"),
        State("transactions-lbl-param2","style"),
        State("transactions-ip-param2","style"),
        State("transactions-lbl-param3","style"),
        State("transactions-ip-param3","style"),
        prevent_initial_callback=True
    )
    def handle_report_selection(rep_name,lbl1_style,ip1_style,lbl2_style,ip2_style,lbl3_style,ip3_style):
        ctx= callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate
        param_props=[
            {
                "Name":"",
                "Visible":False,
                "Value":None,
                "Placeholder":None
            }
            for i in range(len(REPORT_PARAMS[rep_name]))
        ]#initialize param props
        i=0
        #read the report params and udpate param props dict
        for param,values in REPORT_PARAMS[rep_name].items():
            param_props[i]["Name"]=param
            for prop,val in values.items():
                param_props[i][prop]=val
            param_props[i]["Visible"]=True
            i+=1
        #use param props dict to set display values for labels, input boxes to get user inputs for report params.
        #Each param in report_params[rep_name] should get a visble text box and input box with specified placeholders and default values
        elem_props={}
        i=0#iterator
        for param in param_props:
            i+=1 #increment for current param
            #populate label props for the param
            elem_props[param["Name"]]={}
            elem_props[param["Name"]]["Label"]={}
            elem_props[param["Name"]]["Label"]["Children"]=param["Name"]
            lbl_style=locals()[f"lbl{i}_style"]
            lbl_style["visibility"]="visible" if param_props[i]["Visible"] else "hidden"
            elem_props[param["Name"]]["Label"]["Style"]=lbl_style
            #populate input box props for the param    
            elem_props[param["Name"]]["IpBox"]={}
            elem_props[param["Name"]]["IpBox"]["Children"]=param_props[i]["Name"]
            lbl_style=locals()[f"ip{i}_style"]
            lbl_style["visibility"]="visible" if param_props[i]["Visible"] else "hidden"
            elem_props[param["Name"]]["IpBox"]["Style"]=lbl_style            
            elem_props[param["Name"]]["IpBox"]["Placeholder"]=param_props[i]["Placeholder"]
            elem_props[param["Name"]]["IpBox"]["Value"]=param_props[i]["Value"]
        #prepare tuple containing element props to return from the callback
        props_list=[]
        for i in range(len(REPORT_PARAMS[rep_name])):
            #first add the lbl props to the list for the param
            for prop,val in elem_props[f"Param{i}"]["Label"]:
                props_list.append(val)
            #now add the input box props to the list for the param
            for prop,val in elem_props[f"Param{i}"]["IpBox"]:
                props_list.append(val)
        props_list.append(None)#clear any existing reports from the report display div
        return props_list