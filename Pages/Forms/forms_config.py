import dash_bootstrap_components as dbc
from Pages.Forms import accounts_forms
from Pages.ManageData import gold_prices_forms
def register_forms_callbacks(app):
    accounts_forms.register_callbacks(app)
    gold_prices_forms.register_callbacks(app)

def validate_forms():
    #return a list of form layouts for validation in app 
    return[
            accounts_forms.draw_create_form(),
            accounts_forms.draw_update_form(),
            accounts_forms.draw_delete_form(),
            gold_prices_forms.draw_create_form('01-01-2025')
        ]

update_attrib_roles=["filter" ,"update","none"]
default_attrib_role="none"