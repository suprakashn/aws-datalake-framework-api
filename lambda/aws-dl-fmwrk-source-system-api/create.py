import os
import json
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from utils import rollback_src_sys
from api_response import Response


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
        print("Successfully Stored")
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
                print("Successfully stored the credentials")
            else:
                print("Missing Credentials")
        # TODO: Exception Handling for Secrets Manager
        except Exception as e:
            print(e)


def default_ingestion_data(src_sys_id, bucket):
    ing_attributes = {
        'src_sys_id': src_sys_id,
        'ingstn_pattern': 'Not Available',
        'db_type': 'Not Available',
        'db_hostname': 'Not Available',
        'db_username': 'Not Available',
        'db_schema': 'Not Available',
        'db_port': 0000,
        'ingstn_src_bckt_nm': bucket,
        'db_name': 'Not Available'
    }
    return ing_attributes


def store_ingestion_attributes(
    src_sys_id, bucket_name, ingestion_data, ingestion_table, db, region
):
    ingestion_data["src_sys_id"] = src_sys_id
    ingestion_data["ingstn_src_bckt_nm"] = bucket_name
    if "db_pass" in ingestion_data:
        db_secret = ingestion_data["db_pass"]
        ingestion_data.pop("db_pass", None)
    else:
        db_secret = None
    try:
        db.insert(table=ingestion_table, data=ingestion_data)
        store_secret(db_secret, src_sys_id, region)
    except Exception as e:
        print(e)


def create_source_system(method, global_config, src_sys_id, region, db, message_body):
    try:
        src_sys_table = global_config["src_sys_table"]
        ingestion_table = global_config["ingestion_table"]
        bucket_name = f"{global_config['fm_prefix']}-{src_sys_id}-{region}"
        # create folder structure in time and event driven buckets
        time_drvn_bkt = f"{global_config['fm_prefix']}-time-drvn-inbound-{region}"
        event_drvn_bkt = f"{global_config['fm_prefix']}-evnt-drvn-inbound-{region}"

        # TODO: Exception handling related to the folder structure
        create_folder_structure(time_drvn_bkt, src_sys_id)
        create_folder_structure(event_drvn_bkt, src_sys_id)
        # Create source system bucket
        # create_src_bucket(bucket_name, region)
        # TODO: Exception handling related to the CFT
        run_cft(global_config, src_sys_id, region)

        # If source config is present upload the details to the src system table
        # TODO: Exception handling related to the source system table attributes
        src_data = message_body["src_config"]
        src_data["src_sys_id"] = src_sys_id
        src_data["bucket_name"] = bucket_name
        src_data["modified_ts"] = str(datetime.utcnow())
        db.insert(table=src_sys_table, data=src_data)

        # If ingestion config is present
        # 1. upload the details to the src sys ingestion attributes
        # 2. Upload the ingestion DB password to DB secrets
        # TODO: Exception handling related to the ingestion config
        if message_body["ingestion_config"]:
            ingestion_data = message_body["ingestion_config"]
        else:
            ingestion_data = default_ingestion_data(src_sys_id, bucket_name)
        store_ingestion_attributes(
            src_sys_id, bucket_name, ingestion_data, ingestion_table, db, region
        )
        source_system_details = {**src_data, **ingestion_data}
        response = Response(
            method, status=True, body=source_system_details, payload=message_body
        )
    except Exception as e:
        response = Response(
            method, status=False, body=None, payload=message_body, message=str(e)
        )
        rollback_src_sys(db, global_config, src_sys_id, region)
    return response.get_response()
