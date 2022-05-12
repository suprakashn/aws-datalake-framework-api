import json
from datetime import datetime, timezone, timedelta

from connector import Connector
from utils import *

config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
file.close()


def create_source(db, event, context):
    region = os.environ['region']
    # Source Payload
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # generate api response
    api_response = dict()
    api_response['sourcePayload'] = message_body
    api_response['body'] = dict()

    # generate src_sys_id
    src_sys_id = generate_src_sys_id(10)

    # create an entry in the src sys table
    src_sys_table = global_config['src_sys_table']
    ingestion_table = global_config['ingestion_table']
    print(f"Insert source system info in {src_sys_table} table")
    bucket_name = f"{global_config['fm_prefix']}-{src_sys_id}-{region}"
    try:
        # Run the CFT to create source system bucket
        run_cft(global_config, src_sys_id, region)

        # If source config is present upload the details to the src system table
        if message_body['src_config']:
            src_data = message_body['src_config']
            src_data['src_sys_id'] = src_sys_id
            src_data['bucket_name'] = bucket_name
            db.insert(table=src_sys_table, data=src_data)

        # If ingestion config is present upload the details to the src sys ingestion attributes
        if message_body['ingestion_config']:
            ingestion_data = message_body['ingestion_config']
            ingestion_data['src_sys_id'] = src_sys_id
            ingestion_data['bucket_name'] = bucket_name
            db.insert(table=ingestion_table, data=ingestion_data)

        # add the appropriate api response
        api_response['body']['src_sys_created'] = True
        api_response['body']['s3_bucket_created'] = True
        api_response['body']['src_sys_id'] = src_sys_id
        api_response['body']['src_bucket_name'] = bucket_name
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response['sourceCodeDynamoDb'] = response['statusCode']
    except Exception as e:
        print(e)
        rollback_src_sys(db, global_config, src_sys_id, region)
        api_response['body']['src_sys_created'] = False
        api_response['body']['s3_structure_created'] = False
        response = insert_event_to_dynamoDb(event, context, api_call_type, status="Failed")
        api_response['sourceCodeDynamoDb'] = response['statusCode']
    finally:
        return api_response


def read_source(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    src_info = None
    table = global_config['src_sys_table']
    fetch_limit = message_body['fetch_limit']
    src_config = message_body['src_config']
    # if there is no fetch limit, then get the details of the
    # src_sys_id provided in the src_config
    if fetch_limit in [None, 'None', '0', 'NONE'] and src_config:
        src_sys_id = int(src_config['src_sys_id'])
        condition = ("src_sys_id=%s", [src_sys_id])
        src_info = db.retrieve_dict(table=table, cols='all', where=condition)
    # if a fetch limit exists then fetch all the cols of limited src_systems
    elif isinstance(fetch_limit, int) or fetch_limit.isdigit():
        limit = int(fetch_limit)
        src_info = db.retrieve_dict(table=table, cols='all', limit=limit)
    # if neither of the above case satisfies fetch all the info
    elif fetch_limit == 'all':
        src_info = db.retrieve_dict(table=table, cols='all')

    # Generate api response
    api_response = dict()
    api_response['sourcePayload'] = message_body
    api_response['body'] = dict()
    if src_info:
        api_response['body']['exists'] = True
        api_response['body']['src_info'] = src_info
        api_response['statusCode'] = 200
    else:
        api_response['body']['exists'] = False
        api_response['body']['src_info'] = None
        api_response['statusCode'] = 404

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response["sourceCodeDynamoDb"] = response["statusCode"]
    return api_response


def update_source(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # parse payload
    src_config = message_body['src_config']
    ingestion_config = message_body['ingestion_config']
    api_response = dict()
    api_response['sourcePayload'] = src_config
    api_response['body'] = dict()
    try:
        src_exists, src_msg = update_source_system(db, src_config, global_config)
        ing_exists, ing_msg = update_ingestion_attributes(db, ingestion_config, global_config)
        api_response['body']['src_sys_exists'] = src_exists
        api_response['body']['src_update_msg'] = src_msg
        api_response['body']['ingestion_sys_exists'] = ing_exists
        api_response['body']['ingestion_update_msg'] = ing_msg
    except Exception as e:
        print(e)
    finally:
        # API event entry in dynamoDb
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response['sourceCodeDynamoDb'] = response['statusCode']
    return api_response


def delete_source(db, event, context):
    """
    incoming event
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    region = os.environ['region']
    src_config = message_body['src_config']
    # API logic here
    api_response = dict()
    api_response['body'] = dict()
    api_response['sourcePayload'] = message_body
    src_sys_id = int(src_config['src_sys_id'])
    if src_sys_present(db, global_config, src_sys_id):
        api_response['body']['src_sys_present'] = True
        associated = is_associated_with_asset(db, src_sys_id)
        api_response['body']['asset_association'] = associated
        # If it is not associated,source system stack will be deleted
        if not associated:
            try:
                delete_rds_entry(db, global_config, src_sys_id)
                api_response['body']['src_sys_deleted'] = True
            except Exception as e:
                print(e)
            # delete the stack
            delete_src_sys_stack(global_config, src_sys_id, region)
        else:
            api_response['body']['src_sys_deleted'] = False
    else:
        api_response['body']['src_sys_present'] = False
        api_response['body']['src_sys_deleted'] = False

    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response['sourceCodeDynamoDb'] = response["statusCode"]
    return api_response


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]
    db_secret = os.environ['db_secret']
    db_region = os.environ['db_region']
    db = Connector(db_secret, db_region, autocommit=True)

    print(event)
    print(taskType)
    print(method)

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
    db.close()
