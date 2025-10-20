#import python packages
import dash_html_components as html
import dash_bootstrap_components as dbc
import dash
from dash import Input, Output,State,callback_context
#import required project modules
from Pages import styles,shared_components as sc
from CommonDataServices import data_extractor as de
from Pages.Forms import accounts_forms
#draw the accountspage
def draw_page_content(ext=[]):
    accounts_content=html.Div(
        [
            # Display the Sample DataTable from Mongo Db
            html.Div(
                [
                    html.H1("View Account details"),
                    html.H3("Enter search criteria"),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("GL No"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter GL No",id="accounts-search-acc-input-gl-no"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Name"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter customer name",id="accounts-search-acc-input-cust-name"),width=3),
                        ]
                    ),
                    dbc.Row(
                        [
                            dbc.Col(dbc.Label("Customer Phone"),width=2,style={"font-size":"8"}),
                            dbc.Col(dbc.Input(placeholder="Enter Phone No",id="accounts-search-acc-input-cust-phone"),width=3),
                        ]
                    ),
                    dbc.Button("Find accounts",id="accounts-btn-get-account-data"),
                    html.Div(children=None, id="accounts-div-account-details"),
                    html.Hr()
                ],
                style={"margin-bottom":"75px"}, 
            ),
            html.H2("Manage Accounts"),
            dbc.Row(
                [
                    dbc.Col(dbc.Button("Create Account",id="accounts-btn-create-account",disabled=True)),
                    dbc.Col(dbc.Button("Update Account",id="accounts-btn-update-account",disabled=True)),
                    dbc.Col(dbc.Button("Delete Account",id="accounts-btn-delete-account",disabled=True)),
                ],
            ),
            html.Div(children=None, id="accounts-div-manage-acc-forms"),
        ],
        style=styles.CONTENT_STYLE
    )   
    return accounts_content

#Define an register callbacks
def register_callbacks(app):
    @app.callback(
            Output("accounts-div-account-details","children"),
            Input("accounts-btn-get-account-data","n_clicks"),
            State("accounts-search-acc-input-gl-no","value"),
            State("accounts-search-acc-input-cust-name","value"),
            State("accounts-search-acc-input-cust-phone","value"),
    )
    def get_account_data(n_clicks_get,gl_no, cust_name,cust_phone):
        #set mongo db conn details
        attribs=["GLNo","CustName","CustPhone"]
        values=[gl_no,cust_name,cust_phone]
        filters={}
        for attrib,value in zip(attribs,values):
            if value:
                filters[attrib]=value
        if n_clicks_get:
            source_obj=de.source_factory("Accounts",dataset_type="Consumer",db_name="PFS_MI")
            source_obj.load_data(filters)
            # Fetch data from MongoDB and convert to DataFrame
            children=sc.df_to_dash_table(source_obj.data,'accounts-tbl-sample-mflix-comments')
        else:
            children= dash.no_update
        return children

    @app.callback(
            Output("accounts-div-manage-acc-forms","children"),
            Input("accounts-btn-update-account","n_clicks"),
            Input("accounts-btn-create-account","n_clicks"),
            Input("accounts-btn-delete-account","n_clicks"),
    )
    def manage_account_data(n_clicks_update, n_clicks_create,n_clicks_delete):
        trigger = callback_context.triggered[0]
        button_id = trigger["prop_id"].split(".")[0]
        if button_id=="accounts-btn-create-account":
            return accounts_forms.draw_create_form()
        elif button_id=="accounts-btn-update-account":
            return accounts_forms.draw_update_form()
        elif button_id=="accounts-btn-delete-account":
            return accounts_forms.draw_delete_form()
        else:
            return dash.no_update

