from utils import *

from api_response import Response


def read_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_columns = [
        "asset_id",
        "src_sys_id",
        "target_id",
        "file_header",
        "multipartition",
        "file_type",
        "asset_nm",
        "source_path",
        "target_path",
        "trigger_file_pattern",
        "file_delim",
        "file_encryption_ind",
        "athena_table_name",
        "asset_owner",
        "support_cntct",
        "rs_load_ind",
        "rs_stg_table_nm"
    ]

    # Getting the asset id and source system id
    src_sys_id = message_body["src_sys_id"]
    where_clause = ("src_sys_id=%s", [src_sys_id])
    list_asset = database.retrieve_dict(
        table="data_asset",
        cols=asset_columns,
        where=where_clause
    )
    database.close()
    if list_asset:
        status = True
        status_code = 202
        body = list_asset
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
