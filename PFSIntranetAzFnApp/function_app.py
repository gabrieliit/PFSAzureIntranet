import logging
import azure.functions as func
import json
from azure.core.messaging import CloudEvent
import requests



app=func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.event_grid_trigger(arg_name="event")
def EHDashUITestEvent(event:func.EventGridEvent):
    event_dict={
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    }
    result = json.dumps(event_dict)
    #send response to Azure webapp webhook
    webhook_url = f'https://pfsintranet.scm.azurewebsites.net/webhook'
    payload = result # Your payload here
    headers = {'Content-Type': 'application/json'}
    response = requests.post(webhook_url, json=payload, headers=headers)

    # Check the response status
    if response.status_code == 200:
        logging.info('Webhook notified successfully.')
    else:
        logging.info(f'Failed to notify webhook. Status code: {response.status_code}, Reason: {response.reason}')



@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger_Trial1(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger_sample2(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')


@app.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger_sample3(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')

@app.route(route= "http_trigger_sample2")
def http_trigger_sample2(req: func.HttpRequest)->func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse("This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",status_code=200)
        
