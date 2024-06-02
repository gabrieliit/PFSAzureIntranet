#import python packages
import dash_html_components as html
import dash_core_components as dcc
import dash
from dash import Input, Output,State,callback_context
from datetime import datetime,timedelta
import os
import json
import requests
#import required project modules
from Layouts import user_auth_flow as uf,styles
#from Layouts import app_data as ad
event_count=0
#Connection details
hist_period=7
end_date=datetime.today()
start_date=end_date-timedelta(days=hist_period)
end_date=end_date.strftime("%Y-%m-%d")
start_date=start_date.strftime("%Y-%m-%d")
METAL_PRICE_API_KEY=os.environ["METAL_PRICE_API_KEY"]
METAL_PRICE_API_LATEST=f"https://api.metalpriceapi.com/v1/latest?api_key={METAL_PRICE_API_KEY}&base=XAU&currencies=INR"
METAL_PRICE_API_HIST=f"https://api.metalpriceapi.com/v1/timeframe?api_key={METAL_PRICE_API_KEY}&start_date={start_date}&end_date={end_date}&base=XAU&currencies=INR"
GM_PER_TROY_OUNCE=31.103
#draw the homepage
def draw_page_content(ext=[]):
    #define hompage content
    try:
        with open("price.json", "r") as file:
            last_price_point=json.load(file)
            price=last_price_point["price"]
            price="{:.2f}".format(float(price))#format as 2 decimal
            source=last_price_point["source"]
            date=last_price_point["date"]
    except Exception as e:
        price="NA"
        source="NA"
        date="NA"
    homepage_content=html.Div([
        html.Div(
        [
            html.Label("Last fetched gold price :", style={"margin-right":"10px"}),
            dcc.Input(value=f"{price}",style={"margin-right":"10px"},id="home-tb-price-XAU-display",readOnly=True),
            html.Label(f"Sourced from : {source} on {date}",id="home-lbl-price-XAU-source")
        ]),         
        html.Button("Fetch Latest Gold Price", id="home-btn-fetch-xau-latest"),
        html.Div(id="home-div-price-XAU-display"),
        dcc.ConfirmDialog(
            id="home-confirm-dialog-xau-price-persist",
            message="Do you want to save the data to a local file?"
        ),
        dcc.Store(id="home-store-fetched-xau-price-latest")
    ], style=styles.CONTENT_STYLE)   
    return homepage_content

#Define an register callbacks
def register_callbacks(app):
    @app.callback(
        Output("home-store-fetched-xau-price-latest", "data"),
        Output("home-confirm-dialog-xau-price-persist", "displayed"),
        Output("home-tb-price-XAU-display", "value"),
        Output("home-lbl-price-XAU-source", "children"),
        Input("home-btn-fetch-xau-latest", "n_clicks"),
        Input("home-confirm-dialog-xau-price-persist", "submit_n_clicks"),
        State("home-store-fetched-xau-price-latest", "data"),
        prevent_initial_call=True
    )
    def fetch_and_confirm(n_clicks, submit_n_clicks,last_fetched_price):
        ctx = callback_context
        if not ctx.triggered:
            raise dash.exceptions.PreventUpdate

        triggered_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if triggered_id == "home-btn-fetch-xau-latest":
            # Replace with your API URL
            url = METAL_PRICE_API_LATEST
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                price_per_gm=data["rates"]["INR"]/GM_PER_TROY_OUNCE
                date=datetime.today().strftime("%Y-%m-%d")
                price_point={"price":price_per_gm,"date":date,"source" : METAL_PRICE_API_LATEST,}
                return price_point, True, "{:.2f}".format(float(price_per_gm)),f"Sourced from : {METAL_PRICE_API_LATEST} on {date}"
            else:
                try:
                    with open("price.json", "r") as file:
                        last_price_point=json.load(file)
                        price=last_price_point["price"]
                        source=last_price_point["source"]
                        date=last_price_point["date"]
                    return dash.no_update, dash.no_update, dash.no_update,f"Failed to fetch data. Last sourced from : {source} on {date}"
                except:
                    return dash.no_update, dash.no_update, dash.no_update,"Failed to fetch data"

        elif triggered_id == "home-confirm-dialog-xau-price-persist":
            if submit_n_clicks:
                price=last_fetched_price["price"]
                date=last_fetched_price["date"]
                # Save data to local file
                with open("price.json", "w") as f:
                    json.dump(last_fetched_price, f)
                return dash.no_update, False, "{:.2f}".format(float(price)), f"Sourced from {METAL_PRICE_API_LATEST} on {date}"
            return dash.no_update, False, dash.no_update


        



