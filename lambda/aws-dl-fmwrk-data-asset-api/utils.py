from connector import Connector
import boto3
import os
from datetime import datetime
from random import randint
import json


def getGlobalParams():
    print("reading globalConfig")
    with open('config/globalConfig.json', "r") as json_file:
        json_config = json.load(json_file)
        return json_config


def get_database():
    print("connecting to database")
    db_secret = os.environ['secret_name']
    db_region = os.environ['secret_region']
    db_schema = os.environ['stage'] if os.environ['stage'] else 'dev'
    conn = Connector(
        secret=db_secret, region=db_region, autocommit=False, schema=db_schema
    )
    return conn


def generate_asset_id(n):
    print(f"generating {n} digit asset_id")
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
        Key=f"init/{src_sys_id}/{asset_id}/dummy"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"processed/{src_sys_id}/{asset_id}/dummy"
    )
    client.put_object(
        Bucket=bucket_name,
        Key=f"rejected/{src_sys_id}/{asset_id}/dummy"
    )


def glue_airflow_trigger(source_id, asset_id, schedule, email=None):
    print(f"creating dag for asset_id : {asset_id}")
    s3_client = boto3.client("s3")
    template_bucket = 'dl-fmwrk-code-us-east-2'
    env = os.environ['stage']

    template_object_key = f"{env}/aws-datalake-framework-ingestion/airflow/template/dl_fmwrk_dag_template.py"
    dag_id = f"{source_id}_{asset_id}_workflow"
    file_name = f"/mnt/dags/{source_id}_{asset_id}_workflow.py"

    file_content = s3_client.get_object(
        Bucket=template_bucket, Key=template_object_key)["Body"].read()
    file_content = file_content.decode()

    file_content = file_content.replace("env_placeholder", env)
    file_content = file_content.replace("src_sys_id_placeholder", source_id)
    file_content = file_content.replace("ast_id_placeholder", asset_id)
    file_content = file_content.replace("dag_id_placeholder", dag_id)
    if email:
        file_content = file_content.replace("email_placeholder", email)
    if schedule == "None":
        file_content = file_content.replace('"schedule_placeholder"', "None")
    else:
        file_content = file_content.replace("schedule_placeholder", schedule)

    file = bytes(file_content, encoding='utf-8')
    with open(file_name, "wb") as dag_file:
        dag_file.write(file)

    print(
        f"Upload succeeded: {dag_id}.py has been uploaded to Airflow Dags folder in eks")
    return {
        'statusCode': 200,
        'body': f"Upload succeeded: {dag_id}.py has been uploaded to Airflow Dags folder"
    }


def insert_event_to_dynamoDb(event, context, api_call_type, status="success", op_type="insert"):
    print(f"inserting into dynamoDB; status : {status}")
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


def parse_adv_dq(body, asset_id, src_id):
    if 'adv_dq_rules' in body.keys():
        if body['adv_dq_rules']:
            print(f"parsing adv_dq for asset_id : {asset_id}")
            input_rules = body['adv_dq_rules']
            dq_list = list()
            for idx, rule in enumerate(input_rules):
                elem_dict = {
                    'dq_rule_id': f"{src_id}-{asset_id}-{idx}",
                    'asset_id': asset_id,
                    'dq_rule': rule,
                    'created_ts': datetime.utcnow()
                }
                dq_list.append(elem_dict)
            return dq_list
    print("skipped adv dq parsing")
    return None


def delete_adv_dq(db, asset_id):
    where_clause = ("asset_id=%s", [asset_id])
    db.delete(
        table="adv_dq_rules",
        where=where_clause
    )


def update_adv_dq(db, body, src_id, asset_id):
    delete_adv_dq(db, asset_id)
    dq_rules = parse_adv_dq(body, asset_id, src_id)
    print(dq_rules)
    db.insert_many(
        table='adv_dq_rules',
        data=dq_rules
    )


def create_delivery_stream(data_stream_name, src_sys_id, asset_id):
    try:
        my_session = boto3.session.Session()
        my_region = my_session.region_name
        config = getGlobalParams()
        client_kinesis = boto3.client('kinesis', region_name=my_region)
        client_firehose = boto3.client('firehose', region_name=my_region)
        describe_response = client_kinesis.describe_stream(
            StreamName=f'{data_stream_name}',
            Limit=1, )
        data_stream_arn = describe_response['StreamDescription']['StreamARN']
        client_firehose.create_delivery_stream(
            DeliveryStreamName=f'{config["fm_prefix"]}_{src_sys_id}_{asset_id}_delivery_stream',
            DeliveryStreamType='KinesisStreamAsSource',
            KinesisStreamSourceConfiguration={
                'KinesisStreamARN': data_stream_arn,
                'RoleARN': "arn:aws:iam::315119964270:role/dl-fmwrk-kinesis-role",
            },
            S3DestinationConfiguration={
                'RoleARN': "arn:aws:iam::315119964270:role/dl-fmwrk-kinesis-role",
                'BucketARN': f'arn:aws:s3:::{config["fm_prefix"]}-time-drvn-inbound-{my_region}',
                'Prefix': f'init/{src_sys_id}/{asset_id}/' + '!{firehose:random-string}',
                'ErrorOutputPrefix': f'init/{src_sys_id}/{asset_id}/' + '!{firehose:error-output-type}'})
        print("Created delivery strea")
    except Exception as e:
        print(str(e))


def delete_delivery_stream(src_sys_id, asset_id):
    try:
        my_session = boto3.session.Session()
        my_region = my_session.region_name
        config = getGlobalParams()
        client_firehose = boto3.client('firehose', region_name=my_region)
        response = client_firehose.delete_delivery_stream(
            DeliveryStreamName=f'{config["fm_prefix"]}_{src_sys_id}_{asset_id}_delivery_stream'
        )
    except Exception as e:
        print(str(e))
