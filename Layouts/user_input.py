import dash_html_components as html
import dash_core_components as dcc
import dash
from dash.dependencies import Input, Output
import requests
from azure.identity import ClientSecretCredential
from azure.eventgrid import EventGridPublisherClient,EventGridEvent
from azure.core.messaging import CloudEvent

event_count=0
# Define the layout
layout = html.Div([
    html.H1('Dash App'),
    dcc.Input(id='input-data', type='text', placeholder='Enter data'),
    html.Button('SubmitREST', id='submit-button-REST', n_clicks=0),
    html.Div(id='output-message'),
    html.Div(
    [
        html.Label(f"Events published to grid"),
        html.Label(event_count, id='event-count')
    ]
    ),
    html.Button('SubmitEvent', id='submit-button-Event', n_clicks=0),
    html.Label(id="event-pub-status")
])
#credentials
subscription_id = 'your_subscription_id'
resource_group_name = 'your_resource_group_name'
topic_name = 'DashUI'

# Define the callbacks
def register_callbacks(app):
    @app.callback(
        Output('output-message', 'children'),
        [Input('submit-button-REST', 'n_clicks')],
        [dash.dependencies.State('input-data', 'value')]
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
            return ""
    @app.callback(
        [Output('event-count', 'children'),Output('event-pub-status','children')],
        [Input('submit-button-Event', 'n_clicks')],
        [dash.dependencies.State('event-count', 'children')]
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
        function_url = 'https://pfsintranet-azfnapp.azurewebsites.net/api/MyFirstAzFn?code=rnLzSmD_uHrrvGGXbEN5O00tvSlO1bsCe1gj4APzTKuCAzFuO4yeDQ=='
        payload = {'name': string}
        response = requests.post(function_url, json=payload)
        if response.status_code == 200:
            return response.text
        else:
            return None