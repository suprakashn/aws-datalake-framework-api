import boto3
from utils import *

from api_response import Response


def update_asset(event, method, database):
    message_body = event["body-json"]
    payload = message_body.copy()

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    message_keys = message_body.keys()

    try:
        if "asset_info" in message_keys:
            # asset_nm = message_body["asset_info"]["asset_nm"]
            # if "rs_load_ind" in message_body["asset_info"].keys():
            #    rs_load_ind = message_body["asset_info"]["rs_load_ind"]
            #    if rs_load_ind == True:
            #        message_body["asset_info"]["rs_stg_table_nm"] = f"{asset_nm}_stg"
            #    else:
            #        message_body["asset_info"]["rs_stg_table_nm"] = None
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
            if "frequency" in message_body["ingestion_attributes"].keys():
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
        if "adv_dq_rules" in message_keys:
            update_adv_dq(
                database, message_body, src_sys_id, asset_id
            )
        database.close()
        status = True
        status_code = 203

    except Exception as e:
        print(e)
        database.rollback()
        database.close()
        status = False
        status_code = 403
        payload = event["body-json"]
        message_body = {}

    # -----------

    response = Response(
        method=method,
        status=status,
        body=message_body,
        payload=payload
    )
    return response.get_response()
