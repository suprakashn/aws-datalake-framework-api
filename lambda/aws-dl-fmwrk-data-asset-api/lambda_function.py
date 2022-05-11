from genericpath import exists
from sqlite3 import connect
import boto3
import os
import json
from datetime import datetime
from connector import Connector


def getGlobalParams():
    # absolute dir the script is in
    script_dir = os.path.dirname(__file__)
    gbl_cfg_rel_path = "../../config/globalConfig.json"
    gbl_cfg_abs_path = os.path.join(script_dir, gbl_cfg_rel_path)
    with open(gbl_cfg_abs_path) as json_file:
        json_config = json.load(json_file)
        return json_config


def create_src_s3_dir_str(asset_id, message_body):
    global_config = getGlobalParams()

    region = global_config["primary_region"]
    src_sys_id = message_body["asset_info"]["src_sys_id"]
    bucket_name = f"{global_config['fm_prefix']}-{str(src_sys_id)}-{region}"
    print(
        "Creating directory structure in {} bucket".format(bucket_name)
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/init/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/masked/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/error/dummy"'.format(
            bucket_name, asset_id
        )
    )
    os.system(
        'aws s3api put-object --bucket "{}" --key "{}/logs/dummy"'.format(
            bucket_name, asset_id
        )
    )


def insert_event_to_dynamoDb(event, context, api_call_type, status="success", op_type="insert"):
    cur_time = datetime.now()
    aws_request_id = context.aws_request_id
    log_group_name = context.log_group_name
    log_stream_name = context.log_stream_name
    function_name = context.function_name
    method_name = event["context"]["resource-path"]
    query_string = event["params"]["querystring"]
    payload = event["body-json"]

    client = boto3.resource("dynamodb")
    table = client.Table("aws-dl-fmwrk-api-events")

    if op_type == "insert":
        response = table.put_item(
            Item={
                "aws_request_id": aws_request_id,
                "method_name": method_name,
                "log_group_name": log_group_name,
                "log_stream_name": log_stream_name,
                "function_name": function_name,
                "query_string": query_string,
                "payload": payload,
                "api_call_type": api_call_type,
                "modified ts": str(cur_time),
                "status": status,
            })
    else:
        response = table.update_item(
            Key={
                'aws_request_id': aws_request_id,
                'method_name': method_name,
            },
            ConditionExpression="attribute_exists(aws_request_id)",
            UpdateExpression='SET status = :val1',
            ExpressionAttributeValues={
                ':val1': status,
            }
        )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        body = "Insert/Update of the event with aws_request_id=" + \
            aws_request_id + " completed successfully"
    else:
        body = "Insert/Update of the event with aws_request_id=" + aws_request_id + " failed"

    return {
        "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
        "body": body,
    }


def create_asset(event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    asset_id = message_body["asset_info"]["asset_id"]

    # API logic here
    # -----------
    data_dataAsset = message_body["asset_info"]
    data_dataAssetAttributes = message_body["asset_attributes"]
    try:
        Connector.insert(
            table="data_asset",
            data=data_dataAsset
        )
        Connector.insert(
            table="data_asset_attributes",
            data=data_dataAssetAttributes
        )
        status = "200"
        body = {
            "assetId_inserted": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        Connector.rollback()
        body = {
            "error": f"{e}"
        }

    if status == "200":
        create_src_s3_dir_str(asset_id=asset_id, message_body=message_body)

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body,
        "exists": True
    }


def read_asset(event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    if message_body["columns"] != "*":
        column_dict = message_body["columns"]
        columns = list(column_dict.values())
    else:
        columns = message_body["columns"]
    asset_id = message_body["asset_id"]
    limit_number = message_body["limit"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
        dict_dataAsset = Connector.retrieve_dict(
            table="data_asset",
            cols=columns,
            where=where_clause,
            limit=limit_number
        )
        if dict_dataAsset:
            dict_dataAssetAttributes = Connector.retrieve_dict(
                table="data_asset_attributes",
                cols=columns,
                where=where_clause,
                limit=limit_number
            )
        status = "200"
        body = {
            "asset_info": dict_dataAsset,
            "asset_attributes": dict_dataAssetAttributes
        }

    except Exception as e:
        print(e)
        status = "404"
        body = {
            "error": f"{e}"
        }

    # -----------
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def update_asset(event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    data_dataAsset = message_body["asset_info"]
    data_dataAssetAttributes = message_body["asset_attributes"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
        Connector.update(
            table="data_asset",
            data=data_dataAsset,
            where=where_clause
        )
        Connector.update(
            table="data_asset_attributes",
            data=data_dataAssetAttributes,
            where=where_clause
        )
        status = "200"
        body = {
            "updated": {
                "dataAsset": data_dataAsset,
                "dataAssetAttributes": data_dataAssetAttributes
            }
        }

    except Exception as e:
        print(e)
        Connector.rollback()
        status = "404"
        body = {
            "error": f"{e}"
        }

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def delete_asset(event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
        Connector.delete(
            table="data_asset",
            where=where_clause
        )
        Connector.delete(
            table="data_asset_attributes",
            where=where_clause
        )
        status = "200"
        body = {
            "deleted_asset": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        body = {
            "error": f"{e}"
        }
        Connector.rollback()

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourcePayload": message_body,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
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
            return {"statusCode": "200", "body": "API Health is good"}

        elif method == "create":
            response = create_asset(event, context)
            return response

        elif method == "read":
            response = read_asset(event, context)
            return response

        elif method == "update":
            response = update_asset(event, context)
            return response

        elif method == "delete":
            response = delete_asset(event, context)
            return response

        else:
            return {"statusCode": "404", "body": "Not found"}

    Connector.close()
