import json
import os
from random import randrange
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


def generate_src_sys_id(n):
    return int(f"{randrange(1, 10 ** n):03}")


def insert_event_to_dynamoDb(
        event, context, api_call_type, status="success", op_type="insert"
):
    cur_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                "modified ts": cur_time,
                "status": status,
            }
        )
    else:
        response = table.update_item(
            Key={
                "aws_request_id": aws_request_id,
                "method_name": method_name,
            },
            ConditionExpression="attribute_exists(aws_request_id)",
            UpdateExpression="SET status = :val1",
            ExpressionAttributeValues={
                ":val1": status,
            },
        )

    if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        body = (
                "Insert/Update of the event with aws_request_id="
                + aws_request_id
                + " completed successfully"
        )
    else:
        body = (
                "Insert/Update of the event with aws_request_id="
                + aws_request_id
                + " failed"
        )

    return {
        "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
        "body": body,
    }


def src_sys_present(db, global_config, src_sys_id):
    try:
        table = global_config["src_sys_table"]
        condition = ("src_sys_id = %s", [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(table, cols="src_sys_id", where=condition)
        # If item with the specified src_sys_id is present,the response contains "Item" in it
        if response:
            # Returns True if src_sys_id is present
            return True
        else:
            # Returns False if src_sys_id is absent
            return False
    except Exception as e:
        print(e)


def is_associated_with_asset(db, src_sys_id):
    try:
        # Accessing data asset table
        table = "data_asset"
        condition = ("src_sys_id = %s", [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(table, cols="src_sys_id", where=condition)
        if response:
            return True
        else:
            return False
    except Exception as e:
        print(e)


def delete_rds_entry(db, global_config, src_sys_id):
    src_sys_id_int = int(src_sys_id)
    src_table = global_config["src_sys_table"]
    src_ingstn_table = global_config['ingestion_table']
    condition = ("src_sys_id = %s", [src_sys_id_int])
    db.delete(src_table, condition)
    db.delete(src_ingstn_table, condition)


def delete_src_sys_stack(global_config, src_sys_id, region):
    stack_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def rollback_src_sys(db, global_config, src_sys_id, region):
    try:
        delete_src_sys_stack(global_config, src_sys_id, region)
        delete_rds_entry(db, global_config, src_sys_id)
    except Exception as e:
        print(e)
