#import python packages
import dash_core_components as dcc
from dash.dependencies import Input, Output,State
from dash import html
import json
from dash.exceptions import PreventUpdate
import dash
import requests
from azure.identity import ClientSecretCredential
from azure.eventgrid import EventGridPublisherClient
from azure.core.messaging import CloudEvent
#import project modules
from Pages import styles

event_count=0
#draw the scratchpage
def draw_page_content(ext=[]):
    #define hompage content
    scratch_content=html.Div([       
        dcc.Input(id='scratch_input_data', type='text', placeholder='Enter data'),
        html.Button('SubmitREST', id='scratch_submit_button_REST', n_clicks=0),
        html.Div(id='scratch_output_message'),
        html.Div([html.Label(event_count, id='scratch_event_count')]),
        html.Button('SubmitEvent', id='scratch_submit_button_Event', n_clicks=0),
        html.Label(id="scratch_event_pub_status"),
        html.Button('Get Last Post Request',id='scratch_btn_get_last_post_req'),
        html.Label(id="scratch_lbl_last_post_request"),
        html.Div(id="scratch_page_ext",children=ext)
    ], style=styles.CONTENT_STYLE)   
    return scratch_content

#Define the callbacks
def register_callbacks(app):
    @app.callback(
        Output('scratch_output_message', 'children'),
        [Input('scratch_submit_button_REST', 'n_clicks')],
        [State('scratch_input_data', 'value')],
        prevent_initial_call=True
    )
    def make_upper_case(n_clicks, user_input):
        if n_clicks > 0:
            # Create event data with user input
            try:
                #response = send_data_to_azure_function(value)
                response= invoke_REST_API_fn(user_input)
            except Exception:
                #msg ="Azure function call failed"
                msg = "Failed to send event to Azure Event grid"
            if response:
                return "Uppercase Output: " + response.upper()
            else:
                return msg
        else:
            raise PreventUpdate
        
    @app.callback(
        Output("scratch_lbl_last_post_request", 'children'),
        [Input('scratch_btn_get_last_post_req', 'n_clicks')],
        prevent_inital_call=True
    )
    def get_last_post_req(n_clicks):
        if n_clicks is not None and n_clicks > 0:
            # get last post reqeust
            filename="app_data.txt"
            # Read JSON data from a file and convert it into a dictionary
            with open(filename, 'r') as file:
                data = json.load(file)
            return data.post_request
        else:
            return ""
    @app.callback(
        [Output('scratch_event_count', 'children'),Output('scratch_event_pub_status','children')],
        [Input('scratch_submit_button_Event', 'n_clicks')],
        [dash.dependencies.State('scratch_event_count', 'children')],
        prevent_initial_call=True
    )
    def send_event_to_grid(nclicks,event_count):
        pub_status=""
        if nclicks>0:
            event_count+=1
            event_data={"event_count":event_count}
            pub_status=pub_event("test_event",event_data)
        return event_count,pub_status

def pub_event(event_type,event_data):
    topic_endpoint = "https://pfsintranetdashuicloudevents.southindia-1.eventgrid.azure.net/api/events"
    client_id = "33f188e3-4576-4349-b367-08d3cd224619"
    client_secret = "IN_8Q~OQ-tM9A7kLGRtbRh.RLwKDuPhEOPfJzaZX"
    tenant_id = "f60dfc78-fca1-4c4c-9112-b231e45712f9"
    # Authenticate using the service principal
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    pub_client = EventGridPublisherClient(topic_endpoint, credential)
    # Create a CloudEvent object with the event data
    # Create a CloudEvent
    event = CloudEvent\
        (
            type=event_type,
            data=event_data,
            source='http://127.0.0.1:8050/'
         )
    try:
        # Publish the event to the Event Grid topic
        response = pub_client.send(event)
        return f'Event published successfully: {response}'
    except Exception as e:
        return f'Error publishing event: {str(e)}'


def invoke_REST_API_fn(string):
        function_url = 'https://pfsintranet-azfnapp.azurewebsites.net/api/http_trigger_sample2?'
        payload = {'name': string}
        response = requests.post(function_url, json=payload)
        if response.status_code == 200:
            return response.text
        else:
            return None