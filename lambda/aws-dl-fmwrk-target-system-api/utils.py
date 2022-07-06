import os
import json
from datetime import datetime
from random import randrange

import boto3

from connector import Connector, RedshiftConnector
from create import create_target_system
from read import read_target_system
from update import update_target_system
from delete import delete_target_system


def generate_target_sys_id(n):
    """

    :param n:
    :return:
    """
    return int(f'{randrange(1, 10 ** n):03}')


def insert_event_to_dynamoDb(
        event, context, api_call_type, status="success", op_type="insert"
):
    """

    :param event:
    :param context:
    :param api_call_type:
    :param status:
    :param op_type:
    :return:
    """
    cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    aws_request_id = context.aws_request_id
    log_group_name = context.log_group_name
    log_stream_name = context.log_stream_name
    function_name = context.function_name
    method_name = event["context"]["method-path"]
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
                "response_status": status,
            }
        )
    else:
        response = table.update_item(
            Key={
                "aws_request_id": aws_request_id,
                "method_name": method_name,
            },
            ConditionExpression="attribute_exists(aws_request_id)",
            UpdateExpression="SET response_status = :val1",
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


def rollback_target_sys(db, global_config, target_id, region):
    """

    :param db:
    :param global_config:
    :param target_id:
    :param region:
    :return:
    """
    try:
        db.rollback()
        delete_target_sys_stack(global_config, target_id, region)
        delete_rds_entry(db, global_config, target_id)
    except Exception as e:
        print(e)


def delete_target_sys_stack(global_config, target_id, region):
    """

    :param global_config:
    :param target_id:
    :param region:
    :return:
    """
    stack_name = global_config["fm_tgt_prefix"] + "-" + str(target_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def delete_rds_entry(db, global_config, tgt_id):
    """

    :param db:
    :param global_config:
    :param tgt_id:
    :return:
    """
    target_id = int(tgt_id)
    table = global_config['target_sys_table']
    condition = ('target_id = %s', [target_id])
    db.delete(table, condition)
