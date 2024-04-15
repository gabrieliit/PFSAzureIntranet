import azure.functions as func
import json
from azure.core.messaging import CloudEvent
from azure.identity import ClientSecretCredential
from azure.eventgrid import EventGridPublisherClient

def main(event: func.EventGridEvent):
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
    #send response event back to event grid
    topic_endpoint = "https://pfsintranetdashuicloudevents.southindia-1.eventgrid.azure.net/api/events"
    client_id = "33f188e3-4576-4349-b367-08d3cd224619"
    client_secret = "IN_8Q~OQ-tM9A7kLGRtbRh.RLwKDuPhEOPfJzaZX"
    tenant_id = "f60dfc78-fca1-4c4c-9112-b231e45712f9"
    # Authenticate using the service principal
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    pub_client = EventGridPublisherClient(topic_endpoint, credential)
    try:
        # Publish the event to the Event Grid topic
        response = pub_client.send(event)
        return f'Event published successfully: {response}'
    except Exception as e:
        return f'Error publishing event: {str(e)}'


