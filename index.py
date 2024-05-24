# import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash
from dash.dependencies import Input, Output,State
from urllib.parse import parse_qs
from flask import session,url_for
import os
# import required project modules
from Layouts import homepage, webhook,user_auth_flow


def parse_url_params(url):
    # Assuming 'url' is the query string part of the URL
    params = parse_qs(url)
    # Convert the values from list to a single value
    params = {k: v[0] for k, v in params.items()}
    return params

# Register callbacks
def register_callbacks(app):
    #Register callbacks for each page
    homepage.register_callbacks(app)
    #webhook.register_callbacks(app)
    user_auth_flow.register_callbacks(app)
    # register callback to map routes to layouts
    @app.callback(Output('home_layout_extension', 'children'),
                [Input('home_url', 'pathname')],
                State('home_url','search'),
                prevent_initial_call=True
                )
    def display_page(pathname,query):
        # Check if the callback was triggered by a first load
        #if not dash.callback_context.triggered:
            #return homepage.page_layout if pathname=="/" else "404"
        #check if triggered by URL load
        trigger_id = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
        if trigger_id == 'home_url':
            if pathname == '/':
                if "user" not in session:
                    return dcc.Link('Login', href='/login')
                user_info = session["user"]
                if query:
                    # Extract just the query string part after the '?'
                    query_string = query.split('?')[1]
                    parsed_params = parse_url_params(query_string)
                    try:
                        return [html.Label(parsed_params["ext"])]
                    except KeyError:
                        pass              
                else:
                    return []
            elif pathname == '/login':
                return [dcc.Location(id="redirect_to_login",href="/login")]
            elif pathname == '/logout':
                return [dcc.Location(id="redirect_to_logout",href="/logout")]
            # Add more pages as needed
            else:
                return '404'


