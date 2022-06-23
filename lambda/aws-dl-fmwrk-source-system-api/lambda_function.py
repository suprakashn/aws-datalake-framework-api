from datetime import datetime, timezone

from utils import *
from connector import Connector

config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
file.close()


def create_source(db, event, context):
    region = os.environ["region"]
    # Source Payload
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # generate api response
    api_response = dict()
    api_response["sourcePayload"] = message_body
    api_response["body"] = dict()
    # generate a 6 digit source system ID
    src_sys_id = generate_src_sys_id(6)
    # create an entry in the src sys table
    src_sys_table = global_config["src_sys_table"]
    ingestion_table = global_config["ingestion_table"]
    print(f"Insert source system info in {src_sys_table} table")
    bucket_name = f"{global_config['fm_prefix']}-{src_sys_id}-{region}"
    try:
        # create folder structure in time and event driven buckets
        time_drvn_bkt = f"{global_config['fm_prefix']}-time-drvn-inbound-{region}"
        event_drvn_bkt = f"{global_config['fm_prefix']}-evnt-drvn-inbound-{region}"
        create_folder_structure(time_drvn_bkt, src_sys_id)
        create_folder_structure(event_drvn_bkt, src_sys_id)
        # Create source system bucket
        # create_src_bucket(bucket_name, region)
        run_cft(global_config, src_sys_id, region)
        # If source config is present upload the details to the src system table
        if message_body["src_config"]:
            src_data = message_body["src_config"]
            src_data["src_sys_id"] = src_sys_id
            src_data["bucket_name"] = bucket_name
            src_data["modified_ts"] = str(datetime.utcnow())
            db.insert(table=src_sys_table, data=src_data)
        # If ingestion config is present
        # 1. upload the details to the src sys ingestion attributes
        # 2. Upload the ingestion DB password to DB secrets
        if message_body["ingestion_config"]:
            ingestion_data = message_body["ingestion_config"]
        else:
            ingestion_data = default_ingestion_data(src_sys_id, bucket_name)
        store_status = store_ingestion_attributes(
            src_sys_id, bucket_name, ingestion_data, ingestion_table, db, region
        )
        api_response["body"]["store_ingestion_attributes"] = store_status
        # add the appropriate api response
        api_response["body"]["result"] = "Success"
        api_response["body"]["src_sys_created"] = True
        api_response["body"]["s3_bucket_created"] = True
        api_response["body"]["src_sys_id"] = src_sys_id
        api_response["body"]["src_bucket_name"] = bucket_name
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response["sourceCodeDynamoDb"] = response["statusCode"]
    except Exception as e:
        print(e)
        rollback_src_sys(db, global_config, src_sys_id, region)
        api_response["body"]["src_sys_created"] = False
        api_response["body"]["result"] = "Failed"
        response = insert_event_to_dynamoDb(
            event, context, api_call_type, status="Failed"
        )
        api_response["sourceCodeDynamoDb"] = response["statusCode"]
    finally:
        return api_response


def read_source(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    fetch_limit = message_body["fetch_limit"]
    src_config = message_body["src_config"]
    # if there is no fetch limit, then get the details of the
    api_response = read_source_system(db, fetch_limit, src_config, global_config)
    # Generate api response
    api_response["sourcePayload"] = message_body
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response["sourceCodeDynamoDb"] = response["statusCode"]
    return api_response


def update_source(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # parse payload
    src_config = message_body["src_config"]
    ingestion_config = message_body["ingestion_config"]
    api_response = update_source_system(
        db, src_config, global_config, ingestion_config
    )
    api_response["sourcePayload"] = src_config
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response["sourceCodeDynamoDb"] = response["statusCode"]
    return api_response


def delete_source(db, event, context):
    """
    incoming event
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    region = os.environ["region"]
    src_config = message_body["src_config"]
    # API logic here
    api_response = delete_source_system(
        db, global_config, src_config, region
    )
    api_response["sourcePayload"] = message_body
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response["sourceCodeDynamoDb"] = response["statusCode"]
    return api_response


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]
    db_secret = os.environ["db_secret"]
    db_region = os.environ["db_region"]
    db = Connector(db_secret, db_region, autocommit=True)
    print(taskType, method)
    try:
        if event:
            if method == "health":
                return {"statusCode": "200", "body": "API Health is good"}
            elif method == "create":
                response = create_source(db, event, context)
                return response
            elif method == "read":
                response = read_source(db, event, context)
                return response
            elif method == "update":
                response = update_source(db, event, context)
                return response
            elif method == "delete":
                response = delete_source(db, event, context)
                return response
            else:
                return {"statusCode": "404", "body": "Not found"}
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()
