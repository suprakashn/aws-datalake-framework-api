import boto3
from utils import *


def create_asset(event, context, config, database):
    message_body = event["body-json"]
    api_call_type = "synchronous"

    # API logic here
    # -----------

    # getting asset data
    asset_id = str(generate_asset_id(10))
    data_asset = message_body["asset_info"]
    target_id = data_asset["target_id"]
    src_sys_id = data_asset["src_sys_id"]
    data_asset["asset_id"] = asset_id
    data_asset["modified_ts"] = "now()"
    asset_nm = message_body["asset_info"]["asset_nm"]
    rs_load_ind = message_body["asset_info"]["rs_load_ind"]

    # getting ingestion data
    trigger_mechanism = message_body["ingestion_attributes"]["trigger_mechanism"]
    freq = message_body["ingestion_attributes"]["frequency"] if "frequency" in message_body["ingestion_attributes"].keys(
    ) else "None"
    ingestion_attributes = message_body["ingestion_attributes"]
    ingestion_attributes["asset_id"] = asset_id
    ingestion_attributes["src_sys_id"] = src_sys_id
    ingestion_attributes["modified_ts"] = "now()"

    # Getting required data from source_system_ingstn_atrbts table
    ingestion_pattern = database.retrieve_dict(
        table="source_system_ingstn_atrbts",
        cols="ingstn_pattern",
        where=("src_sys_id=%s", [src_sys_id])
    )[0]["ingstn_pattern"]
    if ingestion_pattern == "file":
        if trigger_mechanism == "time_driven":
            ingestion_attributes[
                "ingstn_src_path"
            ] = f"s3://dl-fmwrk-time-drvn-inbound-us-east-2/init/{src_sys_id}/{asset_id}/"
        else:
            ingestion_attributes[
                "ingstn_src_path"
            ] = f"s3://dl-fmwrk-evnt-drvn-inbound-us-east-2/init/{src_sys_id}/{asset_id}/"

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

    athena_table_name = f"{subdomain}_{asset_nm}"
    source_path = f"s3://{bucket_name_source}/{asset_id}/init/"
    target_path = f"s3://{bucket_name_target}/{subdomain}/{asset_id}/"

    data_asset["athena_table_name"] = athena_table_name
    data_asset["target_path"] = target_path
    data_asset["source_path"] = source_path
    if rs_load_ind == True:
        data_asset["rs_stg_table_nm"] = f"{asset_nm}_stg"

    # getting attributes data
    data_asset_attributes = message_body["asset_attributes"]
    for i in data_asset_attributes:
        i["modified_ts"] = "now()"
        i["asset_id"] = asset_id
        i["tgt_col_nm"] = i["col_nm"] if str(
            i["tgt_col_nm"]).lower() == "none" else i["tgt_col_nm"]
        i["tgt_data_type"] = i["data_type"] if str(
            i["tgt_data_type"]).lower() == "none" else i["tgt_data_type"]

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
        database.close()
        status = "200"
        body = {
            "assetId_inserted": asset_id
        }

    except Exception as e:
        print(e)
        status = "404"
        database.rollback()
        database.close()
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

    asset_columns = [
        "asset_id",
        "src_sys_id",
        "target_id",
        "file_header",
        "multipartition",
        "file_type",
        "asset_nm",
        "source_path",
        "target_path",
        "trigger_file_pattern",
        "file_delim",
        "file_encryption_ind",
        "athena_table_name",
        "asset_owner",
        "support_cntct",
        "rs_load_ind",
        "rs_stg_table_nm"
    ]
    attributes_columns = [
        "col_id",
        "asset_id",
        "col_nm",
        "col_desc",
        "data_classification",
        "col_length",
        "req_tokenization",
        "pk_ind",
        "null_ind",
        "data_type",
        "tgt_col_nm",
        "tgt_data_type"
    ]
    ingestion_columns = [
        "asset_id",
        "src_sys_id",
        "src_table_name",
        "src_sql_query",
        "ingstn_src_path",
        "trigger_mechanism",
        "frequency"
    ]
    # Getting the asset id and source system id
    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    # Where clause
    where_clause = ("asset_id=%s", [asset_id])

    try:

        dict_asset = database.retrieve_dict(
            table="data_asset",
            cols=asset_columns,
            where=where_clause
        )[0]
        if dict_asset:
            dict_attributes = database.retrieve_dict(
                table="data_asset_attributes",
                cols=attributes_columns,
                where=where_clause
            )
            dict_ingestion = database.retrieve_dict(
                table="data_asset_ingstn_atrbts",
                cols=ingestion_columns,
                where=(
                    "asset_id=%s and src_sys_id=%s",
                    [asset_id, src_sys_id]
                )
            )[0]
        database.close()
        status = "200"
        body = {
            "asset_info": dict_asset,
            "asset_attributes": dict_attributes,
            "ingestion_attributes": dict_ingestion
        }

    except Exception as e:
        database.close()
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
    message_keys = message_body.keys()

    try:
        if "asset_info" in message_keys:
            asset_nm = message_body["asset_info"]["asset_nm"]
            rs_load_ind = message_body["asset_info"]["rs_load_ind"]
            if rs_load_ind == True:
                message_body["asset_info"]["rs_stg_table_nm"] = f"{asset_nm}_stg"
            else:
                message_body["asset_info"]["rs_stg_table_nm"] = None
            data_dataAsset = message_body["asset_info"]
            data_dataAsset["modified_ts"] = "now()"
            dataAsset_where = ("asset_id=%s", [asset_id])
            database.update(
                table="data_asset",
                data=data_dataAsset,
                where=dataAsset_where
            )
        if "asset_attributes" in message_keys:
            data_dataAssetAttributes = message_body["asset_attributes"]
            for data in data_dataAssetAttributes:
                col_id = data["col_id"]
                data["modified_ts"] = "now()"
                dataAssetAttributes_where = (
                    "asset_id=%s and col_id=%s", [asset_id, col_id])
                database.update(
                    table="data_asset_attributes",
                    data=data,
                    where=dataAssetAttributes_where
                )
        if "ingestion_attributes" in message_keys:
            data_ingestion = message_body["ingestion_attributes"]
            ingestion_where = ("asset_id=%s and src_sys_id=%s", [
                               asset_id, src_sys_id])
            database.update(
                table="data_asset_ingstn_atrbts",
                data=data_ingestion,
                where=ingestion_where
            )
            # Deleting previous dag
            client = boto3.client("s3")
            airflow_bucket = 'dl-fmwrk-mwaa-us-east-2'
            file_name = f"dags/{src_sys_id}_{asset_id}_worflow.py"
            client.delete_object(Bucket=airflow_bucket, Key=file_name)
            # Creating new dag
            freq = message_body["ingestion_attributes"]["frequency"]
            glue_airflow_trigger(
                source_id=src_sys_id,
                asset_id=asset_id,
                schedule=freq
            )
        database.close()
        status = "200"
        body = {
            "assetId_updated": asset_id
        }

    except Exception as e:
        print(e)
        database.rollback()
        database.close()
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
            table="data_asset_catalogs",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
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
        database.close()
        status = "200"
        body = {
            "assetId_deleted": asset_id
        }

        if status == "200":
            bucket_name = "dl-fmwrk-mwaa-us-east-2"
            file_name = f"dags/{src_sys_id}_{asset_id}_workflow.py"
            client = boto3.client('s3')
            client.delete_object(
                Bucket=bucket_name,
                Key=file_name
            )

    except Exception as e:
        print(e)
        status = "404"
        body = {
            "error": f"{e}"
        }
        database.rollback()
        database.close()

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

    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            return {"statusCode": "200", "body": "API Health is good"}

        elif method == "create":
            db = get_database()
            global_config = getGlobalParams()
            response = create_asset(
                event, context, config=global_config, database=db)
            return response

        elif method == "read":
            db = get_database()
            response = read_asset(event, context, database=db)
            return response

        elif method == "update":
            db = get_database()
            response = update_asset(event, context, database=db)
            return response

        elif method == "delete":
            db = get_database()
            response = delete_asset(event, context, database=db)
            return response

        else:
            return {"statusCode": "404", "body": "Not found"}
