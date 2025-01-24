import dash
import flask
import logging
import json
import os
from dash import html
import dash_core_components as dcc
import dash_bootstrap_components as dbc
#import local modules
import Pages.index as index
from Pages import layouts_config
from Pages.Forms import forms_config
from Pages.index import register_callbacks,draw_page_outline


dash_obj = dash.Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
app = dash_obj.server #default azure guincorn startup script target a variable called app to load webapp
app.secret_key = os.urandom(24)
register_callbacks(dash_obj)
# Set the layout - the layout is strucutred as follwos
# 1. a dcc.Location element for a URL Bar
# 2. a skeleton structure with a top banner, and side menu bar
# 3. page content which sits within the skeleton, and is defined in draw_page_content() function of each page
url_bar=dcc.Location(id='home_url',refresh=False)
dash_obj.layout = html.Div([url_bar,index.draw_page_outline()])
dash_obj.validation_layout=[url_bar] + layouts_config.validate_layouts() + forms_config.validate_forms()

@app.route('/webhook',methods=['POST'])
def handle_post():
    req_json=flask.request.get_json()[0]
    print(req_json)
    filename="app_data.txt"
    # Write JSON data into a file
    with open(filename, 'w') as file:
        json.dump(req_json, file, indent=4)
    logging.info(str(req_json)) 
    if req_json['eventType']=="Microsoft.EventGrid.SubscriptionValidationEvent":
            # Check for the validationToken in the event data
            validation_code = req_json['data']['validationCode']
            # Respond with the validation token to complete the validation process
            response=flask.make_response(flask.jsonify({"validationResponse":validation_code})) 
            response.headers['Content-Type'] = 'application/json'
    else: #other events
        req_dict={}
        req_dict["json"]=req_json
        # Convert the headers to a dictionary
        req_dict["headers"]={k: v for k, v in flask.request.headers.items()}
        print(req_dict)
        response=flask.make_response(flask.jsonify(req_dict))
        response.headers["Content-Type"]="application/json"
    return response
    
if __name__ == '__main__':    
    dash_obj.run_server(debug=True)

"""from flask import Flask
app= Flask(__name__)
@app.route("/")
def home():
    return "Hello World - Pushing code from PC to Azure Devops Repo"

if __name__=="__main__":
    app.run(debug=True)
"""

