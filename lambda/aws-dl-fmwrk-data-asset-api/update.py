from utils import *

from api_response import Response


def update_dag(message_body, src_sys_id, asset_id):
    # Deleting previous dag
    dag = f"/mnt/dags/{src_sys_id}_{asset_id}_workflow.py"
    if os.path.exists(dag):
        os.remove(dag)
    # Creating new dag
    freq = "None"
    if "frequency" in message_body["ingestion_attributes"].keys():
        if message_body["ingestion_attributes"]["frequency"] != "":
            freq = message_body["ingestion_attributes"]["frequency"]
    support_email = message_body["asset_info"]["support_cntct"]
    glue_airflow_trigger(
        source_id=str(src_sys_id),
        asset_id=str(asset_id),
        schedule=freq,
        email=support_email
    )


def parse_data_asset_attributes(asset_id, message_body):

    data_asset_attributes = []

    for i in range(len(message_body["asset_attributes"])):
        attribute = {
            "col_id": message_body["asset_attributes"][i]["col_id"],
            "col_nm": message_body["asset_attributes"][i]["col_nm"],
            "col_desc": message_body["asset_attributes"][i]["col_desc"],
            "data_classification": message_body["asset_attributes"][i]["data_classification"],
            "col_length": message_body["asset_attributes"][i]["col_length"],
            "req_tokenization": message_body["asset_attributes"][i]["req_tokenization"],
            "pk_ind": message_body["asset_attributes"][i]["pk_ind"],
            "null_ind": message_body["asset_attributes"][i]["null_ind"],
            "data_type": message_body["asset_attributes"][i]["data_type"]
        }
        attribute["modified_ts"] = datetime.utcnow()
        attribute["asset_id"] = asset_id
        if message_body["asset_attributes"][i]["tgt_col_nm"] == "None":
            attribute["tgt_col_nm"] = attribute["col_nm"]
        else:
            attribute["tgt_col_nm"] = message_body["asset_attributes"][i]["tgt_col_nm"]
        if message_body["asset_attributes"][i]["tgt_data_type"] == "None":
            attribute["tgt_data_type"] = attribute["data_type"]
        else:
            attribute["tgt_data_type"] = message_body["asset_attributes"][i]["tgt_data_type"]

        data_asset_attributes.append(attribute)

    return data_asset_attributes


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
        asset_info["modified_ts"] = datetime.utcnow()
        where_clause = ("asset_id=%s and src_sys_id=%s", [
            asset_id, src_sys_id])
        database.update(
            table="data_asset",
            data=asset_info,
            where=where_clause
        )
        del asset_info["modified_ts"]
        return asset_info
    return {}


def update_asset_attributes(message_body, asset_id, message_keys, database):
    if "asset_attributes" in message_keys:
        # Deleting old data
        database.delete(
            table="data_asset_attributes",
            where=("asset_id=%s", [asset_id])
        )
        # parsing new data
        asset_attributes = parse_data_asset_attributes(asset_id, message_body)
        # Inserting new data
        database.insert_many(
            table="data_asset_attributes",
            data=asset_attributes
        )
        for i in asset_attributes:
            del i["modified_ts"]
        return asset_attributes
    return []


def update_ingestion_attributes(message_body, asset_id, src_sys_id, message_keys, database):
    if "ingestion_attributes" in message_keys:
        ingestion_attributes = message_body["ingestion_attributes"].copy()
        ingestion_attributes["modified_ts"] = datetime.utcnow()
        where_clause = ("asset_id=%s and src_sys_id=%s", [
            asset_id, src_sys_id])
        database.update(
            table="data_asset_ingstn_atrbts",
            data=ingestion_attributes,
            where=where_clause
        )
        del ingestion_attributes["modified_ts"]
        update_dag(message_body, src_sys_id, asset_id)
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
