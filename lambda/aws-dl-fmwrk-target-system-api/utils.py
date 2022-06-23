import os
import json
from random import randrange
from datetime import datetime

import boto3


############### CREATE SECTION ###################
def workgroup_exists(workgroup, region):
    exists = False
    client = boto3.client('athena', region_name=region)
    response = client.list_work_groups()
    workgroups = [i['Name'] for i in response['WorkGroups']]
    if workgroup in workgroups:
        exists = True
    return exists


def create_workgroup(name, region, output_location):
    client = boto3.client('athena', region_name=region)
    client.create_work_group(
        Name=name,
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': output_location
            },
        },
        Description='AWS DL Framework Workgroup',
        Tags=[
            {
                'Key': 'Name',
                'Value': 'AWS DL Framework'
            },
        ]
    )


def create_database(tgt_config, global_config, region):
    ath = boto3.client("athena", region_name=region)
    db_name = tgt_config["domain"]
    wg_name = global_config["workgroup"]
    query = f"create database {db_name}"
    query_path = f"s3://{tgt_config['bucket_name']}/athena/query_results/"
    workgroup = wg_name if workgroup_exists(wg_name, region) \
        else create_workgroup(wg_name, region, query_path)
    response = ath.list_databases(
        CatalogName="AwsDataCatalog",
    )
    if db_name in response["DatabaseList"]:
        print(f"Database {db_name} already exists")
    else:
        ath.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup,
        )


def generate_target_sys_id(n):
    return int(f'{randrange(1, 10 ** n):03}')


def run_cft(global_config, target_id, region):
    target_cft = "cft/targetSystem.yaml"
    with open(target_cft) as yaml_file:
        template_body = yaml_file.read()
    print(
        "Setup source system flow through {}-{}-{} stack".format(
            global_config["fm_tgt_prefix"], str(target_id), region
        )
    )
    stack = boto3.client("cloudformation", region_name=region)
    stack_name = f"{global_config['fm_tgt_prefix']}-{str(target_id)}-{region}"
    stack.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Parameters=[
            {
                "ParameterKey": "CurrentRegion",
                "ParameterValue": region
            },
            {
                "ParameterKey": "DlFmwrkPrefix",
                "ParameterValue": global_config["fm_tgt_prefix"],
            },
            {
                "ParameterKey": "AwsAccount",
                "ParameterValue": os.environ["aws_account"],
            },
            {
                "ParameterKey": "tgtSysId",
                "ParameterValue": str(target_id),
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


############### READ SECTION ###################
def read_target_system(db, fetch_limit, target_config, global_config):
    target_info = None
    cols = ['target_id', 'domain', 'subdomain',
            'bucket_name', 'data_owner', 'support_cntct']
    table = global_config['target_sys_table']
    if fetch_limit in [None, 'None', '0', 'NONE'] and target_config:
        target_id = int(target_config['target_id'])
        condition = ("target_id=%s", [target_id])
        target_info = db.retrieve_dict(table=table, cols=cols, where=condition)
    # if a fetch limit exists then fetch all the cols of limited target_systems
    elif isinstance(fetch_limit, int) or fetch_limit.isdigit():
        limit = int(fetch_limit)
        target_info = db.retrieve_dict(table=table, cols=cols, limit=limit)
    # if neither of the above case satisfies fetch all the info
    elif fetch_limit == 'all':
        target_info = db.retrieve_dict(table=table, cols=cols)
    api_response = dict()
    api_response['body'] = dict()
    if target_info:
        api_response['body']['exists'] = True
        api_response['body']['target_info'] = target_info
        api_response['statusCode'] = 200
    else:
        api_response['body']['exists'] = False
        api_response['body']['target_info'] = None
        api_response['statusCode'] = 404
    return api_response


############### UPDATE SECTION ###################
def update_target_system(db, target_config: dict, global_config: dict):
    api_response = dict()
    api_response['sourcePayload'] = target_config
    api_response['body'] = dict()
    exists, message = None, 'unable to update'
    non_editable_params = ['target_id', 'bucket_name']
    if target_config:
        target_id = target_config['target_id']
        data_to_update = target_config['update_data']
        data_to_update['modified_ts'] = str(datetime.utcnow())
        target_table = global_config['target_sys_table']
        condition = ("target_id=%s", [target_id])
        exists = True if db.retrieve(target_table, 'target_id', condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(target_table, data_to_update, condition)
                message = 'updated'
            else:
                message = "Trying to update a non editable parameter: target_id / bucket name"
        else:
            message = "Target system DNE"
    else:
        message = 'No update config provided'
    api_response['body']['target_sys_exists'] = exists
    api_response['body']['target_update_msg'] = message
    return api_response


############### DELETE SECTION ###################
def get_domain(db, table, tgt_id):
    condition = ("target_id=%s", [tgt_id])
    details = db.retrieve_dict(table, cols=['domain'], where=condition)
    db_name = details[0]['domain']
    return db_name


def rollback_target_sys(db, global_config, target_id, region):
    try:
        db.rollback()
        delete_target_sys_stack(global_config, target_id, region)
        delete_rds_entry(db, global_config, target_id)
    except Exception as e:
        print(e)


def is_associated_with_asset(db, target_id):
    try:
        # Accessing data asset table
        table = 'data_asset'
        condition = ('target_id = %s', [target_id])
        # Trying to get dynamoDB item with target_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='target_id', where=condition
        )
        if response:
            return True
        else:
            return False
    except Exception as e:
        print(e)


def target_present(db, global_config, target_id):
    try:
        table = global_config['target_sys_table']
        condition = ('target_id = %s', [target_id])
        # Trying to get dynamoDB item with target_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='target_id', where=condition
        )
        # If item with the specified target_id is present,the response contains "Item" in it
        if response:
            # Returns True if target_id is present
            return True
        else:
            # Returns False if target_id is absent
            return False
    except Exception as e:
        print(e)


def delete_target_sys_stack(global_config, target_id, region):
    stack_name = global_config["fm_tgt_prefix"] + "-" + str(target_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def delete_rds_entry(db, global_config, tgt_id):
    target_id = int(tgt_id)
    table = global_config['target_sys_table']
    condition = ('target_id = %s', [target_id])
    db.delete(table, condition)


def delete_database(db, tgt_id, global_config, region):
    ath = boto3.client("athena", region_name=region)
    wg_name = global_config["workgroup"]
    table = global_config["target_sys_table"]
    db_name = get_domain(db, table, tgt_id)
    query = f"drop database {db_name}"
    workgroup = wg_name
    response = ath.list_databases(
        CatalogName="AwsDataCatalog",
    )
    print(f"Attempting to delete {db_name}")
    ath.start_query_execution(
        QueryString=query,
        WorkGroup=workgroup,
    )


def delete_target(db, global_config, target_id, region, message_body):
    api_response = dict()
    api_response['body'] = dict()
    api_response['sourcePayload'] = message_body
    if target_present(db, global_config, target_id):
        api_response['body']['target_sys_present'] = True
        associated = is_associated_with_asset(db, target_id)
        api_response['body']['asset_association'] = associated
        # If it is not associated,source system stack will be deleted
        if not associated:
            try:
                delete_rds_entry(db, global_config, target_id)
                delete_target_sys_stack(global_config, target_id, region)
                api_response['statusCode'] = 200
                api_response['body']['target_sys_deleted'] = True
            except Exception as e:
                print(e)
        else:
            api_response['body']['target_sys_deleted'] = False
            api_response['statusCode'] = 400
    else:
        api_response['body']['target_sys_present'] = False
        api_response['body']['target_sys_deleted'] = False
        api_response['statusCode'] = 404
    return api_response
