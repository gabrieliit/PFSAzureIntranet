# import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash
from dash.dependencies import Input, Output,State
from urllib.parse import parse_qs
from flask import session,url_for
import os
# import required project modules
from Layouts import homepage, scratch, shared_components as sc,user_auth_flow,styles


def parse_url_params(url):
    # Assuming 'url' is the query string part of the URL
    params = parse_qs(url)
    # Convert the values from list to a single value
    params = {k: v[0] for k, v in params.items()}
    return params

def draw_page_outline(page_content=[],login=False,un=""):
    #outline defines banner and sidebar which is common skeleton for all pages
    #page content Div contains page specific dash components
    layout=html.Div([
        #dcc.Location(id='home_url',refresh=False), - moved to app.layout
        # Top banner            
        html.Div(
            children=sc.draw_top_banner(login,un),
            style=styles.BANNER_STYLE,
            id="outline_top_banner"),
        # Sidebar menu (customize menu items)
        html.Div([sc.side_bar]),
        html.Div(id="pg_content",children=page_content)
        ])
    return layout

# Register callbacks
def register_callbacks(app):
    #Register callbacks for each page
    homepage.register_callbacks(app)
    scratch.register_callbacks(app)
    #webhook.register_callbacks(app)
    user_auth_flow.register_callbacks(app)
    # register callback to map routes to layouts
    @app.callback([Output('pg_content', 'children'),Output('outline_top_banner','children')],
                [Input('home_url', 'pathname')],
                State('home_url','search'),
                prevent_initial_call=True
                )
    def display_page(pathname,query):
        #whenever url is loaded, check if user is in session, else divert to login page
        if "user" not in session:
            login_state=False
            pg_content=[]
            user_name=""
            if pathname!="/login": #if this is login page, then continue to route, else redirect to skeleton page layout with login link
                return pg_content,dash.no_update
        else:
            login_state=True
            user_name=session["user"]["name"]
        # Check if the callback was triggered by a first load
        #if not dash.callback_context.triggered:
        
        #check if triggered by URL load
        trigger_id = dash.callback_context.triggered[0]['prop_id'].split('.')[0]
        if trigger_id == 'home_url':
            if pathname == '/':
                if query:
                    # Extract just the query string part after the '?'
                    query_string = query.split('?')[1]
                    parsed_params = parse_url_params(query_string)
                    try:
                        home_ext= html.Label(parsed_params["ext"])
                    except KeyError:
                        home_ext= []      
                else:
                    home_ext=[]
                pg_content=homepage.draw_page_content(home_ext)
            elif pathname == '/login':
                pg_content= html.Div([dcc.Location(id="redirect_to_login",href="/login")])
            elif pathname == '/logout':
                pg_content= html.Div([dcc.Location(id="redirect_to_logout",href="/logout")])
            elif pathname=='/scratch':
                pg_content=scratch.draw_page_content()
            # Add more pages as needed
            else:
                pg_content= [html.Label(f'404 - {pathname} this page is under development',style=styles.CONTENT_STYLE)]
            return pg_content,sc.draw_top_banner(login_state,user_name=user_name)


