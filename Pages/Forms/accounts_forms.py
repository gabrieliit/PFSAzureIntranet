import dash_bootstrap_components as dbc
from dash import Input, Output,State
import dash_html_components as html
import dash
import dash_core_components as dcc
#import from other modules
from CommonDataServices import mongo_store as mdb, data_utils as du
from Pages.Forms import forms_config

#set mongo db conn details
db = mdb.mongo_client["PFS_MI"]
collection = db["Accounts"]

def draw_create_form(form_open=True):
    form=dbc.Modal(
        [
            dbc.ModalHeader("Enter Account details"),
            dbc.ModalBody(
                dbc.Form(
                    [
                        dbc.Col(dbc.Input(placeholder="Enter GL No",id="frm-acc-create-input-gl-no"),width=9),
                        dbc.Col(dbc.Input(placeholder="Enter Customer Name",id="frm-acc-create-input-cust-name"),width=9),
                        dbc.Col(dbc.Input(placeholder="Enter Customer Phone No",id="frm-acc-create-input-cust-phone"),width=9),
                    ]
                )
            ),
            dbc.ModalFooter(
                html.Div(
                    [
                        dbc.Button("Submit details", id="create-acc-modal-footer-btn-submit"),
                        dbc.Label(id="create-acc-modal-footer-lbl-submit-result")
                    ]
                )
            )
        ],
        is_open=form_open
    )
    return form

def draw_update_form(form_open=True):
    form=dbc.Modal(
        [
            dbc.ModalHeader("Enter Account details"),
            dbc.ModalBody(
                dbc.Form(
                    [
                        dbc.Row(                       
                            [
                                dbc.Col(dbc.Label("GL No"),width=2,style={"font-size":"8"}),
                                dbc.Col(dbc.Input(placeholder="Enter GL No",id="frm-acc-update-input-gl-no"),width=6),
                                dbc.Col(dcc.Dropdown(forms_config.update_attrib_roles,value=forms_config.default_attrib_role,id="frm-acc-update-ddm-gl-no"),width=3)
                            ],
                        ),                     
                        dbc.Row(
                            [
                                dbc.Col(dbc.Label("Cust Name"),width=2),
                                dbc.Col(dbc.Input(placeholder="Enter Customer Name",id="frm-acc-update-input-cust-name"),width=6),
                                dbc.Col(dcc.Dropdown(forms_config.update_attrib_roles,value=forms_config.default_attrib_role,id="frm-acc-update-ddm-cust-name"),width=3)                            
                            ]
                        ),
                        dbc.Row(
                            [
                                dbc.Col(dbc.Label("Cust Phone"),width=2),
                                dbc.Col(dbc.Input(placeholder="Enter Customer Phone No",id="frm-acc-update-input-cust-phone"),width=6),
                                dbc.Col(dcc.Dropdown(forms_config.update_attrib_roles,value=forms_config.default_attrib_role,id="frm-acc-update-ddm-cust-phone"),width=3)                         
                            ]
                        ),
                        dcc.ConfirmDialog(id="frm-acc-update-cdg-submit-req"),
                    ]
                )
            ),
            dbc.ModalFooter(
                html.Div(
                    [
                        dbc.Button("Submit details", id="update-acc-modal-footer-btn-submit"),
                        dbc.Label(id="update-acc-modal-footer-lbl-submit-result")
                    ]
                )
            )
        ],
        is_open=form_open
    )
    return form

def draw_delete_form(form_open=True):
    form=dbc.Modal(
        [
            dbc.ModalHeader("Enter Account details"),
            dbc.ModalBody(
                dbc.Form(
                    [
                        dbc.Col(dbc.Input(placeholder="Enter GL No",id="frm-acc-delete-input-gl-no"),width=9),
                        dbc.Col(dbc.Input(placeholder="Enter Customer Name",id="frm-acc-delete-input-cust-name"),width=9),
                        dbc.Col(dbc.Input(placeholder="Enter Customer Phone No",id="frm-acc-delete-input-cust-phone"),width=9),
                        dcc.ConfirmDialog(id="frm-acc-delete-cdg-submit-req"),
                    ]
                )
            ),
            dbc.ModalFooter(
                html.Div(
                    [
                        dbc.Button("Submit details", id="delete-acc-modal-footer-btn-submit"),
                        dbc.Label(id="delete-acc-modal-footer-lbl-submit-result")
                    ]
                )
            )
        ],
        is_open=form_open
    )
    return form


def register_callbacks(app):
    @app.callback(
        Output("create-acc-modal-footer-lbl-submit-result","children"),
        Input("create-acc-modal-footer-btn-submit", "n_clicks"),
        State("frm-acc-create-input-gl-no","value"),
        State("frm-acc-create-input-cust-name","value"),
        State("frm-acc-create-input-cust-phone","value"),
    )
    def create_account(n_clicks,gl_no,cust_name,cust_phone):
        if n_clicks:
            doc={
                "GL_No":gl_no,
                "Customer_Name":cust_name,
                "Customer_Phone":cust_phone
            }
            try:
                result= collection.insert_one(doc)
                status= f"Inserted document ID: {result.inserted_id}"
            except Exception as e:
                status=f"Error in inserting doc :  {e}"
        else:
            status= dash.no_update
        return status
    
    @app.callback(
        Output("update-acc-modal-footer-lbl-submit-result","children"),
        Output("frm-acc-update-cdg-submit-req","message"),
        Output("frm-acc-update-cdg-submit-req","displayed"),
        Input("update-acc-modal-footer-btn-submit", "n_clicks"),
        Input("frm-acc-update-cdg-submit-req","submit_n_clicks"),
        State("frm-acc-update-input-gl-no","value"),
        State("frm-acc-update-input-cust-name","value"),
        State("frm-acc-update-input-cust-phone","value"),
        State("frm-acc-update-ddm-gl-no","value"),
        State("frm-acc-update-ddm-cust-name","value"),
        State("frm-acc-update-ddm-cust-phone","value"),
    )
    def update_account_details(n_clicks,submit_n_clicks,gl_no,cust_name,cust_phone,gl_no_role,cust_name_role,cust_phone_role):
        #read all user inputs and prepare filter and update dictionaries
        roles=[gl_no_role,cust_name_role,cust_phone_role]
        values=[gl_no,cust_name,cust_phone]
        attribs=["GL_No","Customer_Name","Customer_Phone"]
        filters={}
        update_vals={}
        for role, value,attrib in zip(roles,values,attribs):
            if role=="filter":
                filters[attrib]=value
            elif role=="update":
                update_vals[attrib]=value
        if n_clicks and not submit_n_clicks:
            # if user clicks submit button, open a confirm dialog
            n_matches=collection.count_documents(filter=filters)
            msg=f"{n_matches} docs match the filter criteria. Are you sure you want to update these records"
            return dash.no_update,msg,True
        elif submit_n_clicks:
            #after user confirms, submit update request to mongdo Db    
            try:
                # Specify the update operation (set a new value for the "address" field)
                update_operation = {"$set": update_vals}
                # Update the document
                result = collection.update_many(filters, update_operation)
                status=f"{result.modified_count} documents were modified"
            except Exception as e:
                status=f"Error in updating doc :  {e}"
        else:
            status= dash.no_update
        return status,dash.no_update,False
    
    @app.callback(
        Output("delete-acc-modal-footer-lbl-submit-result","children"),
        Output("frm-acc-delete-cdg-submit-req","message"),
        Output("frm-acc-delete-cdg-submit-req","displayed"),
        Input("delete-acc-modal-footer-btn-submit", "n_clicks"),
        Input("frm-acc-delete-cdg-submit-req","submit_n_clicks"),
        State("frm-acc-delete-input-gl-no","value"),
        State("frm-acc-delete-input-cust-name","value"),
        State("frm-acc-delete-input-cust-phone","value"),
    )
    def delete_account(n_clicks,submit_n_clicks,gl_no,cust_name,cust_phone):
        #read all user inputs and prepare filter and update dictionaries
        values=[gl_no,cust_name,cust_phone]
        attribs=["GL_No","Cust_Name","Cust_Phone"]
        filters={}
        for value,attrib in zip(values,attribs):
                if value:
                    filters[attrib]=value
        if n_clicks and not submit_n_clicks:
            # if user clicks submit button, open a confirm dialog
            n_matches=collection.count_documents(filter=filters)
            msg=f"{n_matches} docs match the filter criteria.Are you sure you want to delete these records"
            return dash.no_update,msg,True
        elif submit_n_clicks:
            #after user confirms, submit update request to mongdo Db    
            try:
                # Update the document
                result = collection.delete_many(filters)
                status=f"{result.deleted_count} documents were deleted"
            except Exception as e:
                status=f"Error in deleting doc :  {e}"
        else:
            status= dash.no_update
        return status,dash.no_update,False