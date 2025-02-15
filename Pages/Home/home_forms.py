import dash_bootstrap_components as dbc
import dash_core_components as dcc
from dash import html

def draw_create_form(form_open=True):
    form=dbc.Modal(
        [
            dbc.ModalHeader("Enter Gold Price"),
            dbc.ModalBody(
                dbc.Form(
                    [
                        dbc.Col(dbc.Input(placeholder="Enter COB Date (dd-mm-yyyy)",id="frm-home-store-gold-rate-input-cob"),width=3),
                        dbc.Col(dbc.Input(placeholder="Enter Price",id="frm-home-store-gold-rate-input-rate"),width=3),
                        dbc.Col(dcc.Input(id="frm-home-store-gold-rate-input-source",disabled=True),width=3),
                        dbc.Col(dcc.Input(id="SourceType",options="Manual",disabled=True),width=3),
                    ]
                )
            ),
            dbc.ModalFooter(
                html.Div(
                    [
                        dbc.Button("Submit details", id="frm-home-modal-footer-btn-submit"),
                        dbc.Label(id="create-acc-modal-footer-lbl-submit-result")
                    ]
                )
            )
        ],
        is_open=form_open
    )
    return form