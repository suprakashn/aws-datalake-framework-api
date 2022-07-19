from utils import *

from api_response import Response


def read_asset(event, method, database):
    message_body = event["body-json"]

    # API logic here
    # -----------
    try:
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
        if src_sys_id:
            where_clause = ("src_sys_id=%s", [src_sys_id])
            list_asset = database.retrieve_dict(
                table="data_asset",
                cols=asset_columns,
                where=where_clause
            )
        else:
            list_asset = database.retrieve_dict(
                table="data_asset",
                cols=asset_columns
            )
        database.close()
        if list_asset:
            status = True
            body = list_asset
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
        body=body,
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
