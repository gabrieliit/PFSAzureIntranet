from app import dash_obj as app

# Test the POST request handler
def test_post_req_azureEG_validation_event():
    response = app.server.test_client().post(
        'http://127.0.0.1:8050/webhook',
        #'https://pfsintranet.azurewebsites.net/webhook',
        json=
        [
            {
                "id": "2d1781af-3a4c-4d7c-bd0c-e34b19da4e66",
                "topic": "/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "subject": "",
                "data": 
                {
                    "validationCode": "512d38b6-c7b8-40c8-89fe-f46f9e9622b6",
                    "validationUrl": "https://rp-eastus2.eventgrid.azure.net:553/eventsubscriptions/myeventsub/validate?id=0000000000-0000-0000-0000-00000000000000&t=2022-10-28T04:23:35.1981776Z&apiVersion=2018-05-01-preview&token=1A1A1A1A"
                },
                "eventType": "Microsoft.EventGrid.SubscriptionValidationEvent",
                "eventTime": "2022-10-28T04:23:35.1981776Z",
                "metadataVersion": "1",
                "dataVersion": "1"
            }
        ]
    )
    print(f"hi - {response.headers}")
    assert response.status_code == 200
    assert response.json['validationResponse'] == "512d38b6-c7b8-40c8-89fe-f46f9e9622b6"

def test_post_req_non_validation_event():
    response = app.server.test_client().post(
        'http://127.0.0.1:8050/webhook',
        #'https://pfsintranet.azurewebsites.net/webhook',
        json=
        [        
            {
                "id": "2d1781af-3a4c-4d7c-bd0c-e34b19da4e66",
                "topic": "/subscriptions/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                "subject": "TestEvent-NonValidation",
                "data": 
                {
                    "message": "this is a test"
                },
                "eventType": "Microsoft.EventGrid.Event",
                "eventTime": "2022-10-28T04:23:35.1981776Z",
                "metadataVersion": "1",
                "dataVersion": "1"
            }
        ]
    )
    print(f"hi - {response.headers}")
    assert response.status_code == 200
    assert response.json['json']['data']['message'] == "this is a test"