import os
from random import randrange
from datetime import datetime

import boto3


def delete_src_sys_stack(global_config, src_sys_id, region):
    stack_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def delete_rds_entry(db, global_config, src_sys_id):
    src_sys_id_int = int(src_sys_id)
    table = global_config['src_sys_table']
    condition = ('src_sys_id = %s', [src_sys_id_int])
    db.delete(table, condition)


def rollback_src_sys(db, global_config, src_sys_id, region):
    try:
        delete_src_sys_stack(global_config, src_sys_id, region)
        delete_rds_entry(db, global_config, src_sys_id)
    except Exception as e:
        print(e)


def is_associated_with_asset(db, src_sys_id):
    try:
        # Accessing data asset table
        table = 'data_asset'
        condition = ('src_sys_id = %s', [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='src_sys_id', where=condition
        )
        if response:
            return True
        else:
            return False
    except Exception as e:
        print(e)


def src_sys_present(db, global_config, src_sys_id):
    try:
        table = global_config['src_sys_table']
        condition = ('src_sys_id = %s', [src_sys_id])
        # Trying to get dynamoDB item with src_sys_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='src_sys_id', where=condition
        )
        # If item with the specified src_sys_id is present,the response contains "Item" in it
        if response:
            # Returns True if src_sys_id is present
            return True
        else:
            # Returns False if src_sys_id is absent
            return False
    except Exception as e:
        print(e)


def generate_src_sys_id(n):
    return int(f'{randrange(1, 10 ** n):03}')


def run_cft(global_config, src_sys_id, region):
    src_sys_cft = "cft/sourceSystem.yaml"
    with open(src_sys_cft) as yaml_file:
        template_body = yaml_file.read()
    print(
        "Setup source system flow through {}-{}-{} stack".format(
            global_config["fm_prefix"], str(src_sys_id), region
        )
    )
    stack = boto3.client("cloudformation", region_name=region)
    stack_name = f"{global_config['fm_prefix']}-{str(src_sys_id)}-{region}"
    response = stack.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Parameters=[
            {"ParameterKey": "CurrentRegion", "ParameterValue": region},
            {
                "ParameterKey": "DlFmwrkPrefix",
                "ParameterValue": global_config["fm_prefix"],
            },
            {
                "ParameterKey": "AwsAccount",
                "ParameterValue": os.environ["aws_account"],
            },
            {
                "ParameterKey": "srcSysId",
                "ParameterValue": str(src_sys_id),
            },
        ],
    )


def insert_event_to_dynamoDb(
    event, context, api_call_type, status="success", op_type="insert"
):
    cur_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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


def update_source_system(db, src_config: dict, global_config: dict):
    exists, message = None, 'unable to update'
    non_editable_params = ['src_sys_id', 'bucket_name']
    if src_config:
        src_sys_id = src_config['src_sys_id']
        data_to_update = src_config['update_data']
        src_sys_table = global_config['src_sys_table']
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = True if db.retrieve(src_sys_table, 'src_sys_id', condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(src_sys_table, data_to_update, condition)
                message = 'updated'
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source system DNE"
    else:
        message = 'No update config provided'
    return exists, message


def update_ingestion_attributes(db, ingestion_config: dict, global_config: dict):
    exists, message = None, 'unable to update'
    non_editable_params = ['src_sys_id', 'ingstn_src_bckt_nm']
    if ingestion_config:
        src_sys_id = ingestion_config['src_sys_id']
        data_to_update = ingestion_config['update_data']
        ingestion_table = global_config['ingestion_table']
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = True if db.retrieve(ingestion_table, 'src_sys_id', condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(ingestion_table, data_to_update, condition)
                message = 'updated'
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source System DNE"
    else:
        message = 'No update config provided'
    return exists, message

