from utils import *

from api_response import Response


def read_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------
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
        "tgt_file_path"
    ]
    # Getting the asset id and source system id
    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    # Where clause
    where_clause = ("asset_id=%s and src_sys_id=%s", [asset_id, src_sys_id])
    dict_catalog = database.retrieve_dict(
        table="data_asset_catalogs",
        cols=catalog_columns,
        where=where_clause
    )
    database.close()
    if dict_catalog:
        status = True
        status_code = 202
        body = dict_catalog
    else:
        status = False
        status_code = 402
        body = {}
    # -----------
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status_code,
        "status": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body,
        "payload": message_body
    }


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]
    db = get_database()

    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            response = Response(method, True, "body", None)
            return response.get_response()

        elif method == "read":
            db = get_database()
            generated_response = read_asset(event, context, database=db)
            response = Response(
                method=method,
                status=generated_response["status"],
                body=generated_response["body"],
                payload=generated_response["payload"]
            )
            return response.get_response()
        else:
            return {"statusCode": "404", "body": "Not found"}
