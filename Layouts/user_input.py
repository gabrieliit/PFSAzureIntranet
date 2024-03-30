import dash_html_components as html
import dash_core_components as dcc
import dash
from dash.dependencies import Input, Output
import requests

# Define the layout
layout = html.Div([
    html.H1('Dash App'),
    dcc.Input(id='input-data', type='text', placeholder='Enter data'),
    html.Button('Submit', id='submit-button', n_clicks=0),
    html.Div(id='output-message')
])

# Define the callbacks
def register_callbacks(app):
    @app.callback(
        Output('output-message', 'children'),
        [Input('submit-button', 'n_clicks')],
        [dash.dependencies.State('input-data', 'value')]
    )
    def update_output(n_clicks, value):
        if n_clicks > 0:
            try:
                response = send_data_to_azure_function(value)
            except Exception:
                msg ="Azure function call failed"
            if response:
                return "Uppercase Output: " + response.upper()
            else:
                return msg
        else:
            return ""

    def send_data_to_azure_function(data):
        function_url = 'https://pfsintranet-azfnapp.azurewebsites.net/api/MyFirstAzFn?code=rnLzSmD_uHrrvGGXbEN5O00tvSlO1bsCe1gj4APzTKuCAzFuO4yeDQ=='
        payload = {'name': data}
        response = requests.post(function_url, json=payload)
        if response.status_code == 200:
            return response.text
        else:
            return None
