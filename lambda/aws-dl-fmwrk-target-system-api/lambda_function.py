from utils import *
from connector import Connector, RedshiftConnector
from create import create_target_system
from read import read_target_system
from update import update_target_system
from delete import delete_target_system

config_file_path = "config/globalConfig.json"
file = open(file=config_file_path, mode="r")
global_config = json.load(file)
file.close()


def create_target_sys(
        method, metadata_conn, redshift_conn, event, context
):
    """
    create a target system
    """
    region = os.environ['region']
    message_body = event["body-json"]
    api_call_type = "synchronous"
    target_id = generate_target_sys_id(6)
    target_sys_table = global_config['target_sys_table']
    print(f"Insert source system info in {target_sys_table} table")
    bucket_name = f"{global_config['fm_tgt_prefix']}-{target_id}-{region}"
    if message_body['target_config']:
        target_data = message_body['target_config']
        target_data['target_id'] = target_id
        target_data['bucket_name'] = bucket_name
        target_data['modified_ts'] = str(datetime.utcnow())
    else:
        target_data = None
    api_response = create_target_system(
        method, global_config, region, metadata_conn,
        redshift_conn, message_body, target_data
    )
    if api_response['responseStatus']:
        insert_event_to_dynamoDb(event, context, api_call_type, 'SUCCESS')
    else:
        insert_event_to_dynamoDb(event, context, api_call_type, 'FAILED')
    return api_response


def read_target_sys(method, metadata_db, event, context):
    """
    Read a target system details
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    fetch_limit = message_body['fetch_limit']
    source_payload = message_body['target_config']
    api_response = read_target_system(
        method, metadata_db, fetch_limit, source_payload, global_config
    )
    insert_event_to_dynamoDb(event, context, api_call_type)
    return api_response


def update_target_sys(method, metadata_db, redshift_db, event, context):
    """
     update a target system
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    # parse payload
    api_response = None
    target_config = message_body['target_config']
    try:
        api_response = update_target_system(
            method, metadata_db, target_config, global_config
        )
    except Exception as e:
        print(e)
    finally:
        # API event entry in dynamoDb
        response = insert_event_to_dynamoDb(event, context, api_call_type)
        api_response['sourceCodeDynamoDb'] = response['statusCode']
    return api_response


def delete_target_sys(method, metadata_db, redshift_db, event, context):
    """
    delete a target system
    """
    message_body = event["body-json"]
    api_call_type = "synchronous"
    region = os.environ['region']
    target_config = message_body['target_config']
    # API logic here
    target_id = int(target_config['target_id'])
    api_response = delete_target_system(
        method, metadata_db, redshift_db, global_config, target_id, region, message_body
    )
    insert_event_to_dynamoDb(event, context, api_call_type)
    return api_response


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]
    db_secret = os.environ['db_secret']
    rs_secret = os.environ['rs_secret']
    region = os.environ['region']
    metadata_conn = Connector(db_secret, region, autocommit=True)
    if "rs_load_ind" in event["body-json"]["target_config"]:
        if event["body-json"]["target_config"]["rs_load_ind"]:
            redshift_conn = RedshiftConnector(
                'dev', secret=rs_secret, region=region,
                autocommit=True, create_db=True
            )
        else:
            redshift_conn = None
    else:
        redshift_conn = None
    print(event)
    print(taskType)
    print(method)
    try:
        if event:
            if method == "health":
                return {"statusCode": "200", "body": "API Health is good"}
            elif method == "create":
                response = create_target_sys(
                    method, metadata_conn, redshift_conn, event, context
                )
                return response
            elif method == "read":
                response = read_target_sys(
                    method, metadata_conn, event, context
                )
                return response
            elif method == "update":
                response = update_target_sys(
                    method, metadata_conn, redshift_conn, event, context
                )
                return response
            elif method == "delete":
                response = delete_target_sys(
                    method, metadata_conn, redshift_conn, event, context
                )
                return response
            else:
                return {"statusCode": "404", "body": "Not found"}
    except Exception as e:
        print(e)
        metadata_conn.rollback()
    finally:
        metadata_conn.close()
