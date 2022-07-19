from utils import *

from api_response import Response
import json


def read_asset(event, method, database):
    message_body = event["body-json"]

    # API logic here
    # -----------
    try:
        catalog_columns = [
            "exec_id",
            "src_sys_id",
            "asset_id",
            "dq_validation",
            "data_publish",
            "data_masking",
            "dq_validation_exec_id",
            "data_publish_exec_id",
            "data_masking_exec_id",
            "src_file_path",
            "s3_log_path",
            "tgt_file_path",
            "proc_start_ts"
        ]
        # Getting the asset id and source system id
        asset_id = message_body["asset_id"]
        src_sys_id = message_body["src_sys_id"]
        # Where clause
        where_clause = ("asset_id=%s and src_sys_id=%s",
                        [asset_id, src_sys_id])
        dict_catalog = database.retrieve_dict(
            table="data_asset_catalogs",
            cols=catalog_columns,
            where=where_clause
        )
        database.close()
        if dict_catalog:
            status = True
            body = dict_catalog
        else:
            status = False
            body = {}
    except Exception as e:
        body = str(e)
        status = False
        database.close()
    # -----------
    response = Response(
        method=method,
        status=status,
        body=json.loads(
            json.dumps(body, indent=4, sort_keys=True, default=str)
        ),
        payload=message_body
    )
    return response.get_response()


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

        elif method == "read":
            response = read(event, context, method)
            return response
        else:
            return {"statusCode": "404", "body": "Not found"}
