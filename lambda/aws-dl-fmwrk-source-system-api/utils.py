import json
import os
from random import randrange
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError


########################## CREATE SECTION #############################
def create_src_bucket(bucket_name, region=None):
    try:
        if region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client("s3", region_name=region)
            location = {"LocationConstraint": region}
            s3_client.create_bucket(
                Bucket=bucket_name, CreateBucketConfiguration=location
            )
    except ClientError as e:
        print(e)
        return False
    return True


def generate_src_sys_id(n):
    return int(f"{randrange(1, 10 ** n):03}")


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


def create_folder_structure(bucket, src_id):
    client = boto3.client("s3")
    dummy_file = "dummy.txt"
    init_key = f"init/{src_id}/{dummy_file}"
    processed_key = f"processed/{src_id}/{dummy_file}"
    rejected_key = f"rejected/{src_id}/{dummy_file}"
    body = b"Creating the folder structure"
    client.put_object(Body=body, Bucket=bucket, Key=init_key)
    client.put_object(Body=body, Bucket=bucket, Key=processed_key)
    client.put_object(Body=body, Bucket=bucket, Key=rejected_key)


def secret_exists(secret_id, region):
    client = boto3.client(service_name="secretsmanager", region_name=region)
    secret_list = client.list_secrets()["SecretList"]
    secrets = [i["Name"] for i in secret_list]
    if secret_id in secrets:
        return True
    else:
        return False


def store_secret(db_secret, src_sys_id, region):
    secret_id = f"dl-fmwrk-ingstn-db-secrets-{src_sys_id}"
    if secret_exists(secret_id, region):
        store_status = "Credential already exists"
    else:
        try:
            client = boto3.client(service_name="secretsmanager", region_name=region)
            if db_secret:
                secret = {f"{src_sys_id}": f"{db_secret}"}
                secret_string = json.dumps(secret)
                client.create_secret(
                    Name=secret_id,
                    Description="Credentials of Ingestion DB",
                    SecretString=secret_string,
                    Tags=[
                        {"Key": "src_sys_id", "Value": f"{src_sys_id}"},
                    ],
                )
                store_status = f"Stored the credentials for src sys: {src_sys_id}"
            else:
                store_status = f"Missing credentials for src sys"
        except Exception as e:
            store_status = e
    return store_status


def default_ingestion_data(src_sys_id, bucket):
    ing_attributes = {
        'src_sys_id': src_sys_id,
        'ingstn_pattern': 'Not Available',
        'db_type': 'Not Available',
        'db_hostname': 'Not Available',
        'db_username': 'Not Available',
        'db_schema': 'Not Available',
        'db_port': None,
        'ingstn_src_bckt_nm': bucket,
        'db_name': 'Not Available'
    }
    return ing_attributes


def store_ingestion_attributes(
        src_sys_id, bucket_name, ingestion_data, ingestion_table, db, region
):
    store_status = ""
    ingestion_data["src_sys_id"] = src_sys_id
    ingestion_data["ingstn_src_bckt_nm"] = bucket_name
    if "db_pass" in ingestion_data:
        db_secret = ingestion_data["db_pass"]
        ingestion_data.pop("db_pass", None)
    else:
        db_secret = None
    try:
        db.insert(table=ingestion_table, data=ingestion_data)
        store_status = "Stored the ingestion Attributes and "
        store_status += store_secret(db_secret, src_sys_id, region)
    except Exception as e:
        store_status = e
    return store_status


########################## READ SECTION #############################
def read_source_system(db, fetch_limit, src_config, global_config):
    src_sys_table = global_config["src_sys_table"]
    ingestion_table = global_config["ingestion_table"]
    api_response = dict()
    api_response["body"] = dict()
    response = None
    default_sql = f"""
            SELECT A.src_sys_id,A.bucket_name,A.src_sys_nm,A.src_sys_desc,
            A.mechanism,A.data_owner,A.support_cntct,
            B.ingstn_pattern,B.db_type,B.db_hostname,B.db_username,
            B.db_schema,B.db_port,B.ingstn_src_bckt_nm,B.db_name
            FROM {src_sys_table} A
            LEFT JOIN {ingestion_table} B
            ON A.src_sys_id=B.src_sys_id
            """
    if fetch_limit in [None, "None", "0", "NONE", "null"] and src_config:
        src_sys_id = src_config["src_sys_id"]
        if isinstance(src_sys_id, list):
            src_sys_ids = ','.join(map(str, src_sys_id)).rstrip(',')
            sql = default_sql + f" WHERE A.src_sys_id in ({src_sys_ids})"
            response = db.execute(sql, return_type='dict')
        else:
            sql = default_sql + f" WHERE A.src_sys_id={int(src_sys_id)}"
            response = db.execute(sql, return_type='dict')
    # if a fetch limit exists then fetch all the cols of limited src_systems
    elif isinstance(fetch_limit, int) or fetch_limit.isdigit():
        limit = int(fetch_limit)
        sql = default_sql + f" LIMIT {limit}"
        response = db.execute(sql, return_type='dict')
    # if neither of the above case satisfies fetch all the info
    elif fetch_limit == "all":
        sql = default_sql
        response = db.execute(sql, return_type='dict')
    if response:
        api_response["body"]["exists"] = True
        api_response["body"]["src_info"] = response
        api_response["statusCode"] = 200
    else:
        api_response["body"]["exists"] = False
        api_response["body"]["src_info"] = None
        api_response["statusCode"] = 404
    return api_response


########################## UPDATE SECTION #############################
def update_src(db, src_config: dict, global_config: dict):
    exists, message = None, "unable to update"
    non_editable_params = ["src_sys_id", "bucket_name"]
    if src_config:
        src_sys_id = src_config["src_sys_id"]
        data_to_update = src_config["update_data"]
        data_to_update['modified_ts'] = str(datetime.utcnow())
        src_sys_table = global_config["src_sys_table"]
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = True if db.retrieve(src_sys_table, "src_sys_id", condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(src_sys_table, data_to_update, condition)
                message = "updated"
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source system DNE"
    else:
        message = "No update config provided"
    return exists, message


def update_ing(db, ingestion_config: dict, global_config: dict):
    exists, message = None, "unable to update"
    non_editable_params = ["src_sys_id", "ingstn_src_bckt_nm"]
    if ingestion_config:
        src_sys_id = ingestion_config["src_sys_id"]
        data_to_update = ingestion_config["update_data"]
        data_to_update['modified_ts'] = str(datetime.utcnow())
        ingestion_table = global_config["ingestion_table"]
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = (
            True if db.retrieve(ingestion_table, "src_sys_id", condition) else False
        )
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(ingestion_table, data_to_update, condition)
                message = "updated"
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source System DNE"
    else:
        message = "No update config provided"
    return exists, message


def update_source_system(db, src_config, global_config, ingestion_config):
    api_response = dict()
    api_response["body"] = dict()
    src_exists, src_msg = update_src(db, src_config, global_config)
    ing_exists, ing_msg = update_ing(
        db, ingestion_config, global_config
    )
    api_response["body"]["src_sys_exists"] = src_exists
    api_response["body"]["src_update_msg"] = src_msg
    api_response["body"]["ingestion_sys_exists"] = ing_exists
    api_response["body"]["ingestion_update_msg"] = ing_msg
    return api_response


########################## DELETE SECTION #############################
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


def delete_source_system(db, global_config, src_config, region):
    api_response = dict()
    api_response["body"] = dict()
    src_sys_id = int(src_config["src_sys_id"])
    if src_sys_present(db, global_config, src_sys_id):
        api_response["body"]["src_sys_present"] = True
        associated = is_associated_with_asset(db, src_sys_id)
        api_response["body"]["asset_association"] = associated
        # If it is not associated,source system stack will be deleted
        if not associated:
            try:
                delete_rds_entry(db, global_config, src_sys_id)
                api_response["body"]["src_sys_deleted"] = True
            except Exception as e:
                print(e)
            # delete the stack
            delete_src_sys_stack(global_config, src_sys_id, region)
        else:
            api_response["body"]["src_sys_deleted"] = False
    else:
        api_response["body"]["src_sys_present"] = False
        api_response["body"]["src_sys_deleted"] = False
    return api_response


def rollback_src_sys(db, global_config, src_sys_id, region):
    try:
        delete_src_sys_stack(global_config, src_sys_id, region)
        delete_rds_entry(db, global_config, src_sys_id)
    except Exception as e:
        print(e)
