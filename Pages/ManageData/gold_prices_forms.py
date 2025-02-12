import dash_bootstrap_components as dbc
from dash import Input, Output,State
import dash_html_components as html
import dash
import pandas as pd
from flask import session
#import from other modules
from CommonDataServices import mongo_store as mdb, data_utils as du
from Pages.Forms import forms_config
from ConsumerServices.DatasetTools.DatasetDefs import gold_prices

def draw_create_form(cob_date,form_open=True):
    try:
        username=session['user']['name']
    except RuntimeError:
        username='No User'
    form = dbc.Modal(
        [
            dbc.ModalHeader("Enter Gold Price Details"),
            dbc.ModalBody(
                dbc.Form(
                    [
                        dbc.Row(
                            [
                                dbc.Label("COB Date", width=4),
                                dbc.Col(
                                    dbc.Input(value=cob_date, id="frm-xau-create-input-cob"),
                                    width=8,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Label("Price", width=4),
                                dbc.Col(
                                    dbc.Input(placeholder="Enter Price", id="frm-xau-create-input-price"),
                                    width=8,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Label("Source Type", width=4),
                                dbc.Col(
                                    dbc.Input(value="Manual Entry", id="frm-xau-create-input-src-type", disabled=True),
                                    width=8,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Label("Source", width=4),
                                dbc.Col(
                                    dbc.Input(placeholder="Enter Source of price, eg MetalPriceAPI.com", id="frm-xau-create-input-src"),
                                    width=8,
                                ),
                            ],
                            className="mb-3",
                        ),
                        dbc.Row(
                            [
                                dbc.Label("Username", width=4),
                                dbc.Col(
                                    dbc.Input(value=f"{username}", id="frm-xau-create-input-un"),
                                    width=8,
                                ),
                            ],
                            className="mb-3",
                        ),
                    ]
                )
            ),
            dbc.ModalFooter(
                dbc.Row(
                    [
                        dbc.Col(
                            dbc.Button("Submit details", id="modal-xau-create-footer-btn-submit"),
                            width=8,
                            className="d-flex justify-content-end"
                        ),
                        dbc.Col(
                            dbc.Label(id="modal-xau-create-footer-lbl-submit-result"),
                            width=8,
                            className="d-flex justify-content-end"
                        )
                    ]
                )
            )
        ],
        is_open=form_open
    )
    return form

def register_callbacks(app):
    @app.callback(
        Output("modal-xau-create-footer-lbl-submit-result","children"),
        Input("modal-xau-create-footer-btn-submit", "n_clicks"),
        State("frm-xau-create-input-cob","value"),
        State("frm-xau-create-input-src-type","value"),
        State("frm-xau-create-input-src","value"),
        State("frm-xau-create-input-price","value"),
        State("frm-xau-create-input-un","value"),
    )
    def create_record(n_clicks,cob,src_type,src,price,un):
        if n_clicks:
            try:
                result= gold_prices.set_gold_price(float(price),pd.to_datetime(cob),src,src_type,un)
                status= f"Inserted doc ID:{str(result['InsertedID'])} "
            except Exception as e:
                status=f"Error in inserting doc :  {e}"
        else:
            status= dash.no_update
        return status