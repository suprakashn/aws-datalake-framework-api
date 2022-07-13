from utils import *

from api_response import Response


def parse_data(message_body, database):
    # getting asset data
    asset_id = generate_asset_id(10)
    data_asset = message_body["asset_info"]
    target_id = data_asset["target_id"]
    src_sys_id = data_asset["src_sys_id"]
    data_asset["asset_id"] = asset_id
    data_asset["modified_ts"] = "now()"
    asset_nm = message_body["asset_info"]["asset_nm"]
    rs_load_ind = message_body["asset_info"]["rs_load_ind"]
    support_email = message_body["asset_info"]["support_cntct"]

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

    # get adv_dq_rules
    adv_dq_rules = parse_adv_dq(
        message_body, asset_id, src_sys_id
    )
    return asset_id, src_sys_id, trigger_mechanism, data_asset, data_asset_attributes,\
        ingestion_attributes, adv_dq_rules, support_email, freq


def create_asset(event, method, config, database):
    payload = event["body-json"]

    # API logic here
    # -----------

    asset_id, src_sys_id, trigger_mechanism, data_asset, data_asset_attributes, ingestion_attributes, adv_dq_rules, \
        support_email, freq = parse_data(payload.copy(), database)

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
        if adv_dq_rules:
            database.insert_many(
                table='adv_dq_rules',
                data=adv_dq_rules
            )
        database.close()
        status_code = 200
        status = True
        # creating body without ts
        del data_asset["modified_ts"]
        for i in data_asset_attributes:
            del i["modified_ts"]
        del ingestion_attributes["modified_ts"]
        for i in adv_dq_rules:
            del i["created_ts"]
        body = {
            "data_asset": data_asset,
            "data_asset_attributes": data_asset_attributes,
            "data_asset_ingstn_atrbts": ingestion_attributes,
            "adv_dq_rules": adv_dq_rules
        }

    except Exception as e:
        print(e)
        status_code = 401
        status = False
        body = str(e)
        database.rollback()
        database.close()

    finally:
        if status_code == 200:
            try:
                create_src_s3_dir_str(
                    asset_id=asset_id,
                    message_body=payload,
                    config=config,
                    mechanism=trigger_mechanism
                )
            except Exception as e:
                status_code = 401
                status = False
                body = {"s3_dir_error": str(e)}
        if status_code == 200:
            try:
                response = glue_airflow_trigger(
                    asset_id=str(asset_id),
                    source_id=src_sys_id,
                    schedule=freq,
                    email=support_email
                )
            except Exception as e:
                status_code = 401
                status = False
                body = {"airflow_error": str(e)}

    # -----------

    response = Response(
        method=method,
        status=status,
        body=body,
        payload=payload
    )
    return response.get_response()
