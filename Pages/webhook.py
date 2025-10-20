
# import python packages
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
# import required project modules
#from Layouts import app_data
from Pages.Home import home_layout
import flask
import logging


#Define layout (defined within a function in case it needs to be paramterised later to accept layout elements as params)
page_layout = home_layout.page_layout


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
        
    
 