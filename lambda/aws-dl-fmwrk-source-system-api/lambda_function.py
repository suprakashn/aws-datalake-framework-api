from datetime import datetime, timezone

from connector import Connector
from utils import *
from create import create_source_system
from read import read_source_system
from update import update_source_system
from delete import delete_source_system

config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
file.close()


def create_source(method, db, event, context):
    region = os.environ["region"]
    message_body = event["body-json"]
    api_call_type = "synchronous"
    src_sys_id = generate_src_sys_id(6)
    api_response = create_source_system(
        method, global_config, src_sys_id, region, db, message_body
    )
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def read_source(method, db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # if there is no fetch limit, then get the details of the
    api_response = read_source_system(
        method, db, message_body, global_config
    )
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def update_source(method, db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    api_response = update_source_system(
        method, db, message_body, global_config
    )
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type)
    else:
        insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
    return api_response


def delete_source(method, db, event, context):
    """
    incoming event
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    region = os.environ["region"]
    # API logic here
    api_response = delete_source_system(
        method, db, global_config, message_body, region
    )
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
    db_secret = os.environ["db_secret"]
    db_region = os.environ["db_region"]
    db = Connector(db_secret, db_region, autocommit=True)
    print(taskType, method)
    try:
        if event:
            if method == "health":
                return {"statusCode": "200", "body": "API Health is good"}
            elif method == "create":
                response = create_source(method, db, event, context)
                return response
            elif method == "read":
                response = read_source(method, db, event, context)
                return response
            elif method == "update":
                response = update_source(method, db, event, context)
                return response
            elif method == "delete":
                response = delete_source(method, db, event, context)
                return response
            else:
                return {"statusCode": "404", "body": "Not found"}
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()
