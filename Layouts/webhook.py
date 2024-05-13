
# import python packages
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
# import required project modules
from Layouts import app_data
from Layouts import homepage
import flask


#Define layout (defined within a function in case it needs to be paramterised later to accept layout elements as params)
layout = homepage.layout


#Define the callbacks
def register_callbacks(app):
    @app.callback(
        Output('home_url', 'href'),
        [Input('home_url', 'search')]
    )
    def webhook_handler(payload):
        #Extracts the query params passed with the webhook url, and passes it to home page url via a URL query param
        # Extract just the query string part after the '?'
        if payload:
            query_string = payload.split('?')[1]
            return f"/?ext={query_string}"
        else:
            return f"/"
        
    @app.server.route('/webhook',methods=['POST'])
    def handle_post():
        data=flask.request.json
        try:#handle post from azure validation event 
            # Check for the validationToken in the event data
            validation_token = data['data']['validationToken']
            # Respond with the validation token to complete the validation process
            return flask.jsonify(validationResponse=validation_token)
        except KeyError:#handle post from other events
            app_data.post_request["json"]=data
            app_data.post_request["headers"]=flask.request.headers
            return 'Event received',200