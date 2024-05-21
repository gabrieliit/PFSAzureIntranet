import dash
from Layouts import homepage
from index import register_callbacks
import flask
import logging
import json

dash_obj = dash.Dash(__name__)
app = dash_obj.server #default azure guincorn startup script target a variable called app to load webapp
# Set the layout
dash_obj.layout = homepage.layout
register_callbacks(dash_obj)

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

