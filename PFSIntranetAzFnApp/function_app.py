import azure.functions as func
import json
from azure.core.messaging import CloudEvent
from azure.identity import ClientSecretCredential
from azure.eventgrid import EventGridPublisherClient

app=func.FunctionApp

@app.function_name(name="EHDashUITestEvent")
@app.event_grid_trigger(arg_name="event")
def main(event:func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })
    #create response event
    event = CloudEvent \
    (
        source="https://pfsintranet-azfnapp.azurewebsites.net/runtime/webhooks/EventGrid?functionName=EHDashUITestEvent&code=SNN3nOoTWKfxZb4KnlqhMxmQ9RknKRwupW7MWyUNqoNOAzFukWYaFQ==",
        type="fn.response.test",
        data=result
    )
    return f'Cloud Event object : {event.__dict__}'


