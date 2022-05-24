from genericpath import exists
from sqlite3 import connect
import boto3
import os
import json
from datetime import datetime
from connector import Connector
from random import randint


def getGlobalParams():
    with open('globalConfig.json', "r") as json_file:
        json_config = json.load(json_file)
        return json_config


def get_database():
    db_secret = os.environ['secret_name']
    db_region = os.environ['secret_region']
    conn = Connector(secret=db_secret, region=db_region, autocommit=False)
    return conn


def generate_asset_id(n):
    range_start = 10**(n-1)
    range_end = (10**n)-1
    return randint(range_start, range_end)


def create_src_s3_dir_str(asset_id, message_body, config, mechanism):

    region = config["primary_region"]
    src_sys_id = message_body["asset_info"]["src_sys_id"]
    bucket_name = f"{config['fm_prefix']}-{str(src_sys_id)}-{region}"
    print(
        "Creating directory structure in {} bucket".format(bucket_name)
    )
    client = boto3.client('s3')
    client.put_object(
        Bucket=bucket_name,
        Key=f"{asset_id}/init/dummy"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{asset_id}/error/dummy"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{asset_id}/masked/dummy"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{asset_id}/logs/dummy"
    )

    if mechanism == "time_driven":
        bucket_name = f"{config['fm_prefix']}-time-drvn-inbound-{region}"
    else:
        bucket_name = f"{config['fm_prefix']}-evnt-drvn-inbound-{region}"
    print(
        "Creating directory structure in {} bucket".format(bucket_name)
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{src_sys_id}/{asset_id}/init/"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{src_sys_id}/{asset_id}/processed/"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"{src_sys_id}/{asset_id}/rejected/"
    )


def glue_airflow_trigger(source_id, asset_id, schedule):
    s3_client = boto3.client("s3")
    template_bucket = 'dl-fmwrk-code-us-east-2'
    airflow_bucket = 'dl-fmwrk-mwaa-us-east-2'

    template_object_key = "airflow-template/dl_fmwrk_dag_template.py"
    dag_id = f"{source_id}_{asset_id}_worflow"
    file_name = f"dags/{source_id}_{asset_id}_worflow.py"

    file_content = s3_client.get_object(
        Bucket=template_bucket, Key=template_object_key)["Body"].read()
    file_content = file_content.decode()

    file_content = file_content.replace("src_sys_id_placeholder", source_id)
    file_content = file_content.replace("ast_id_placeholder", asset_id)
    file_content = file_content.replace("dag_id_placeholder", dag_id)
    if schedule == "None":
        file_content = file_content.replace('"schedule_placeholder"', "None")
    else:
        file_content = file_content.replace("schedule_placeholder", schedule)

    file = bytes(file_content, encoding='utf-8')
    s3_client.put_object(Bucket=airflow_bucket, Body=file, Key=file_name)

    return {
        'statusCode': 200,
        'body': f"Upload succeeded: {file_name} has been uploaded to Amazon S3 in bucket {airflow_bucket}"
    }


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


def create_asset(event, context, config, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------
    asset_id = str(generate_asset_id(10))
    trigger_mechanism = message_body["ingestion_attributes"]["trigger_mechanism"]
    freq = message_body["ingestion_attributes"]["frequency"]

    # getting asset data
    data_asset = message_body["asset_info"]
    target_id = data_asset["target_id"]
    src_sys_id = data_asset["src_sys_id"]
    data_asset["asset_id"] = asset_id
    data_asset["modified_ts"] = "now()"
    # Getting required data from target and source sys tables
    target_data = database.retrieve_dict(
        table="target_system",
        cols=["subdomain", "bucket_name"],
        where=("target_id=%s", [target_id])
    )[0]
    subdomain = target_data["subdomain"]
    bucket_name_target = target_data["bucket_name"]

    source_data = database.retrieve_dict(
        table="source_system",
        cols="bucket_name",
        where=("src_sys_id=%s", [src_sys_id])
    )[0]
    bucket_name_source = source_data["bucket_name"]

    athena_table_name = f"{subdomain}_{asset_id}"
    source_path = f"s3://{bucket_name_source}/{asset_id}/init/"
    target_path = f"s3://{bucket_name_target}/{subdomain}/{asset_id}/"

    data_asset["athena_table_name"] = athena_table_name
    data_asset["target_path"] = target_path
    data_asset["source_path"] = source_path

    # getting attributes data
    data_asset_attributes = message_body["asset_attributes"]
    data_asset_attributes = list(data_asset_attributes.values())
    for i in data_asset_attributes:
        i["modified_ts"] = "now()"
        i["asset_id"] = asset_id
        i["tgt_col_nm"] = i["col_nm"]
        i["tgt_data_type"] = i["data_type"]

    # getting ingestion data
    ingestion_attributes = message_body["ingestion_attributes"]
    ingestion_attributes["asset_id"] = asset_id
    ingestion_attributes["src_sys_id"] = src_sys_id
    ingestion_attributes["modified_ts"] = "now()"

    try:
        database.insert(
            table="data_asset",
            data=data_asset
        )
        database.insert_many(
            table="data_asset_attributes",
            data=data_asset_attributes
        )
        database.insert(
            table="data_asset_ingstn_atrbts",
            data=ingestion_attributes
        )
        status = "200"
        body = {
            "assetId_inserted": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        database.rollback()
        body = {
            "error": f"{e}"
        }

    finally:
        if status == "200":
            try:
                create_src_s3_dir_str(
                    asset_id=asset_id,
                    message_body=message_body,
                    config=config,
                    mechanism=trigger_mechanism
                )
                body["s3_dir_creation"] = "success"
            except Exception as e:
                status = "s3_dir_error"
                body = {
                    "error": f"{e}"
                }
        if status == "200":
            try:
                response = glue_airflow_trigger(
                    asset_id=asset_id,
                    source_id=src_sys_id,
                    schedule=freq
                )
                body["airflow"] = response
            except Exception as e:
                status = "airflow_error"
                body = {
                    "error": f"{e}"
                }

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body,
    }


def read_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    # Getting the column info
    if message_body["asset_info"]["columns"] != "*":
        column_dict = message_body["asset_info"]["columns"]
        asset_columns = list(column_dict.values())
    else:
        asset_columns = message_body["asset_info"]["columns"]
    if message_body["asset_attributes"]["columns"] != "*":
        column_dict = message_body["asset_attributes"]["columns"]
        attributes_columns = list(column_dict.values())
    else:
        attributes_columns = message_body["asset_attributes"]["columns"]
    if message_body["ingestion_attributes"]["columns"] != "*":
        column_dict = message_body["ingestion_attributes"]["columns"]
        ingestion_columns = list(column_dict.values())
    else:
        ingestion_columns = message_body["ingestion_attributes"]["columns"]
    # Getting the asset id and source system id
    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    # Getting the limit
    assetAttributes_limit = None if (message_body["asset_attributes"]["limit"]).lower() == "none" else int(
        message_body["asset_attributes"]["limit"])
    # Where clause
    where_clause = ("asset_id=%s", [asset_id])

    try:

        dict_asset = database.retrieve_dict(
            table="data_asset",
            cols=asset_columns,
            where=where_clause
        )
        if dict_asset:
            dict_attributes = database.retrieve_dict(
                table="data_asset_attributes",
                cols=attributes_columns,
                where=where_clause,
                limit=assetAttributes_limit
            )
            dict_ingestion = database.retrieve_dict(
                table="data_asset_ingstn_atrbts",
                cols=ingestion_columns,
                where=(
                    "asset_id=%s and src_sys_id=%s",
                    [asset_id, src_sys_id]
                )
            )
        status = "200"
        body = {
            "asset_info": dict_asset,
            "asset_attributes": dict_attributes,
            "ingestion_attributes": dict_ingestion
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
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def update_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    try:
        body = {
            "updated": {}
        }
        message_keys = message_body.keys()
        if "asset_info" in message_keys:
            data_dataAsset = message_body["asset_info"]
            data_dataAsset["modified_ts"] = "now()"
            dataAsset_where = ("asset_id=%s", [asset_id])
            database.update(
                table="data_asset",
                data=data_dataAsset,
                where=dataAsset_where
            )
            body["updated"]["asset_info"] = data_dataAsset
        if "asset_attributes" in message_keys:
            data_dataAssetAttributes = message_body["asset_attributes"]
            for col in data_dataAssetAttributes.keys():
                col_id = database.retrieve_dict(
                    table="data_asset_attributes",
                    cols="col_id",
                    where=("col_nm=%s", [col])
                )[0]["col_id"]
                col_data = {
                    k: v for k, v in data_dataAssetAttributes[col].items() if k != "column_id"
                }
                col_data["modified_ts"] = "now()"
                dataAssetAttributes_where = (
                    "asset_id=%s and col_id=%s", [asset_id, col_id])
                database.update(
                    table="data_asset_attributes",
                    data=col_data,
                    where=dataAssetAttributes_where
                )
            body["updated"]["asset_attributes"] = data_dataAssetAttributes
        if "ingestion_attributes" in message_keys:
            data_ingestion = message_body["ingestion_attributes"]
            ingestion_where = ("asset_id=%s and src_sys_id=%s", [
                               asset_id, src_sys_id])
            database.update(
                table="data_asset_ingstn_atrbts",
                data=data_ingestion,
                where=ingestion_where
            )
            body["updated"]["ingestion_attributes"] = data_ingestion
        status = "200"

    except Exception as e:
        print(e)
        database.rollback()
        status = "404"
        body = {
            "error": f"{e}"
        }

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
    }


def delete_asset(event, context, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    where_clause = ("asset_id=%s", [asset_id])

    try:
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
        database.rollback()

    # -----------

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    return{
        "statusCode": status,
        "sourceCodeDynamoDb": response["statusCode"],
        "body": body
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
            return {"statusCode": "200", "body": "API Health is good"}

        elif method == "create":
            global_config = getGlobalParams()
            response = create_asset(
                event, context, config=global_config, database=db)
            db.close()
            return response

        elif method == "read":
            response = read_asset(event, context, database=db)
            db.close()
            return response

        elif method == "update":
            response = update_asset(event, context, database=db)
            db.close()
            return response

        elif method == "delete":
            response = delete_asset(event, context, database=db)
            db.close()
            return response

        else:
            return {"statusCode": "404", "body": "Not found"}
