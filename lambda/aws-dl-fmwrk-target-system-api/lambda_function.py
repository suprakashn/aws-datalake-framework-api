from connector import Connector
from utils import *

config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
file.close()


def create_target_sys(db, event, context):
    region = os.environ['region']
    # Source Payload
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # generate api response
    api_response = dict()
    api_response['sourcePayload'] = message_body
    api_response['body'] = dict()
    # generate target_id
    target_id = generate_target_sys_id(6)
    # create an entry in the target sys table
    target_sys_table = global_config['target_sys_table']
    print(f"Insert source system info in {target_sys_table} table")
    bucket_name = f"{global_config['fm_tgt_prefix']}-{target_id}-{region}"
    try:
        # Run the CFT to create source system bucket
        run_cft(global_config, target_id, region)
        # If source config is present upload the details to the target system table
        if message_body['target_config']:
            target_data = message_body['target_config']
            target_data['target_id'] = target_id
            target_data['bucket_name'] = bucket_name
            target_data['modified_ts'] = str(datetime.utcnow())
            create_database(target_data, global_config, region)
            db.insert(table=target_sys_table, data=target_data)
        # add the appropriate api response
        api_response['body']['target_sys_created'] = True
        api_response['body']['s3_bucket_created'] = True
        api_response['body']['target_id'] = target_id
        api_response['body']['target_bucket_name'] = bucket_name
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response['sourceCodeDynamoDb'] = response['statusCode']
        api_response['statusCode'] = 200
    except Exception as e:
        print(e)
        rollback_target_sys(db, global_config, target_id, region)
        api_response['body']['target_sys_created'] = False
        api_response['body']['s3_structure_created'] = False
        response = insert_event_to_dynamoDb(event, context, api_call_type, status="Failed")
        api_response['sourceCodeDynamoDb'] = response['statusCode']
        api_response['statusCode'] = 400
    finally:
        return api_response


def read_target_sys(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # API logic here
    fetch_limit = message_body['fetch_limit']
    target_config = message_body['target_config']
    # if there is no fetch limit, then get the details of the
    # target_id provided in the target_config
    api_response = read_target_system(
        db, fetch_limit, target_config, global_config
    )
    api_response['sourcePayload'] = message_body
    # API event entry in dynamoDb
    response = insert_event_to_dynamoDb(event, context, api_call_type)
    api_response["sourceCodeDynamoDb"] = response["statusCode"]
    return api_response


def update_target_sys(db, event, context):
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # parse payload
    api_response = None
    target_config = message_body['target_config']
    try:
        api_response = update_target_system(
            db, target_config, global_config
        )
    except Exception as e:
        print(e)
    finally:
        # API event entry in dynamoDb
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response['sourceCodeDynamoDb'] = response['statusCode']
    return api_response


def delete_target_sys(db, event, context):
    """
    incoming event
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    region = os.environ['region']
    target_config = message_body['target_config']
    # API logic here
    target_id = int(target_config['target_id'])
    api_response = delete_target(
        db, global_config, target_id, region,  message_body
    )
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
    try:
        if event:
            if method == "health":
                return {"statusCode": "200", "body": "API Health is good"}
            elif method == "create":
                response = create_target_sys(db, event, context)
                return response
            elif method == "read":
                response = read_target_sys(db, event, context)
                return response
            elif method == "update":
                response = update_target_sys(db, event, context)
                return response
            elif method == "delete":
                response = delete_target_sys(db, event, context)
                return response
            else:
                return {"statusCode": "404", "body": "Not found"}
    except Exception as e:
        print(e)
        db.rollback()
    finally:
        db.close()
