import boto3
from utils import *


def delete_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    payload = message_body.copy()

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
        database.delete(
            table="data_asset_catalogs",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
        database.delete(
            table="adv_dq_rules",
            where=where_clause
        )
        database.delete(
            table="data_asset_attributes",
            where=where_clause
        )
        database.delete(
            table="data_asset_ingstn_atrbts",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
        database.delete(
            table="data_asset",
            where=where_clause
        )
        database.close()
        status = True
        status_code = 204

        if status == 204:
            bucket_name = "dl-fmwrk-mwaa-us-east-2"
            file_name = f"dags/{src_sys_id}_{asset_id}_workflow.py"
            client = boto3.client('s3')
            client.delete_object(
                Bucket=bucket_name,
                Key=file_name
            )

    except Exception as e:
        print(e)
        status = False
        status_code = 404
        database.rollback()
        database.close()

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status_code,
        "status": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": message_body,
        "payload": payload
    }
