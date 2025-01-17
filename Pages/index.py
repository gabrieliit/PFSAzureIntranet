# import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash
from dash.dependencies import Input, Output,State
from urllib.parse import parse_qs
from flask import session,url_for
import os
# import required project modules
from Pages import scratch, shared_components as sc
from Pages import user_auth_flow,styles,layouts_config,dataloader,load_jobs_inv,accounts,customers,transactions
from Pages.Forms import forms_config
from Pages.Home import home_layout


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
    #Register callbacks for each layout
    layouts_config.register_layout_callbacks(app)
    #register forms callbacks
    forms_config.register_forms_callbacks(app)
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
                pg_content=home_layout.draw_page_content(home_ext)
            elif pathname == '/login':
                pg_elements= [dcc.Location(id="redirect_to_login",href="/login")]
                if query:
                    # Extract just the query string part after the '?'
                    query_string = query.split('?')[1]
                    parsed_params = parse_url_params(query_string)
                    try:#handle error messages returned from microsoft oauth post request
                        error_msg=parsed_params["error"]
                        pg_elements=pg_elements.append(html.Label(error_msg))
                    except:
                        pass
                pg_content= html.Div(pg_elements)                    
            elif pathname == '/logout':
                pg_content= html.Div([dcc.Location(id="redirect_to_logout",href="/logout")])
            elif pathname=='/scratch':
                pg_content=scratch.draw_page_content()
            elif pathname=='/dataloader':
                pg_content=dataloader.draw_page_content()
            elif pathname=='/dataloadjobs':
                pg_content=load_jobs_inv.draw_page_content()
            elif pathname=='/accounts':
                pg_content=accounts.draw_page_content()
            elif pathname=='/customers':
                pg_content=customers.draw_page_content()
            elif pathname=='/transactions':
                pg_content=transactions.draw_page_content()
            # Add more pages as needed
            else:
                pg_content= [html.Label(f'404 - {pathname} this page is under development',style=styles.CONTENT_STYLE)]
            return pg_content,sc.draw_top_banner(login_state,user_name=user_name)


