# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(blueprint) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints
import logging
import azure.functions as func

blueprint = func.Blueprint()


@blueprint.event_grid_trigger(arg_name="azeventgrid")
def EventGridTrigger_sample4(azeventgrid: func.EventGridEvent):
    logging.info('Python EventGrid trigger processed an event')



@blueprint.route(route="blueprint, auth_level=func.AuthLevel.ANONYMOUS")
def blueprint(req: func.HttpRequest) -> func.HttpResponse:
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
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )