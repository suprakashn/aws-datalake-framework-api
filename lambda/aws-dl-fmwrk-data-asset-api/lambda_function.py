from utils import *

from create import create_asset
from read import read_asset
from update import update_asset
from delete import delete_asset


def create(event, context, method):
    database = get_database()
    config = getGlobalParams()
    api_response = create_asset(event, method, config, database)
    # API event entry in dynamoDb
    api_call_type = "synchronous"
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def read(event, context, method):
    database = get_database()
    api_response = read_asset(event, method, database)
    # API event entry in dynamoDb
    api_call_type = "synchronous"
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def update(event, context, method):
    database = get_database()
    api_response = update_asset(event, method, database)
    # API event entry in dynamoDb
    api_call_type = "synchronous"
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def delete(event, context, method):
    database = get_database()
    api_response = delete_asset(event, method, database)
    # API event entry in dynamoDb
    api_call_type = "synchronous"
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]

    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            return {"statusCode": "200", "body": "API Health is good"}

        elif method == "create":
            response = create(event, context, method)
            return response

        elif method == "read":
            response = read(event, context, method)
            return response

        elif method == "update":
            response = update(event, context, method)
            return response

        elif method == "delete":
            response = delete(event, context, method)
            return response

        else:
            return {"statusCode": "404", "body": "Not found"}
