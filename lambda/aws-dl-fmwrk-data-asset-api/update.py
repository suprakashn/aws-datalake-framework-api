import boto3
from utils import *

from api_response import Response


def update_asset_info(message_body, asset_id, src_sys_id, message_keys, database):
    if "asset_info" in message_keys:
        # asset_nm = message_body["asset_info"]["asset_nm"]
        # if "rs_load_ind" in message_body["asset_info"].keys():
        #    rs_load_ind = message_body["asset_info"]["rs_load_ind"]
        #    if rs_load_ind == True:
        #        message_body["asset_info"]["rs_stg_table_nm"] = f"{asset_nm}_stg"
        #    else:
        #        message_body["asset_info"]["rs_stg_table_nm"] = None
        asset_info = message_body["asset_info"].copy()
        asset_info["modified_ts"] = "now()"
        where_clause = ("asset_id=%s and src_sys_id=%s", [
            asset_id, src_sys_id])
        database.update(
            table="data_asset",
            data=asset_info,
            where=where_clause
        )
        return asset_info
    return {}


def update_asset_attributes(message_body, asset_id, message_keys, database):
    if "asset_attributes" in message_keys:
        asset_attributes = message_body["asset_attributes"].copy()
        for data in asset_attributes:
            col_id = data["col_id"]
            data["modified_ts"] = "now()"
            where_clause = (
                "asset_id=%s and col_id=%s", [asset_id, col_id])
            database.update(
                table="data_asset_attributes",
                data=data,
                where=where_clause
            )
        return asset_attributes
    return []


def update_ingestion_attributes(message_body, asset_id, src_sys_id, message_keys, database):
    if "ingestion_attributes" in message_keys:
        ingestion_attributes = message_body["ingestion_attributes"].copy()
        where_clause = ("asset_id=%s and src_sys_id=%s", [
            asset_id, src_sys_id])
        database.update(
            table="data_asset_ingstn_atrbts",
            data=ingestion_attributes,
            where=where_clause
        )
        if "frequency" in message_body["ingestion_attributes"].keys():
            # Deleting previous dag
            os.remove(f"/mnt/dags/{src_sys_id}_{asset_id}_worflow.py")
            # Creating new dag
            freq = message_body["ingestion_attributes"]["frequency"]
            glue_airflow_trigger(
                source_id=str(src_sys_id),
                asset_id=str(asset_id),
                schedule=freq
            )
        return ingestion_attributes
    return {}


def update_asset(event, method, database):
    message_body = event["body-json"]

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]
    message_keys = message_body.keys()

    try:
        asset_info = update_asset_info(
            message_body,
            asset_id,
            src_sys_id,
            message_keys,
            database
        )
        asset_attributes = update_asset_attributes(
            message_body,
            asset_id,
            message_keys,
            database
        )
        ingestion_attributes = update_ingestion_attributes(
            message_body,
            asset_id,
            src_sys_id,
            message_keys,
            database
        )

        data_adv_dq = []
        if "adv_dq_rules" in message_keys:
            update_adv_dq(
                database, message_body, src_sys_id, asset_id
            )
            data_adv_dq = message_body["adv_dq_rules"]
        database.close()
        status = True
        body = {
            "asset_info": asset_info,
            "asset_attributes": asset_attributes,
            "ingestion_attributes": ingestion_attributes,
            "adv_dq": data_adv_dq
        }
    except Exception as e:
        print(e)
        database.rollback()
        database.close()
        status = False
        body = {}

    # -----------

    response = Response(
        method=method,
        status=status,
        body=body,
        payload=message_body
    )
    return response.get_response()
