from utils import *

from api_response import Response


def parse_asset_info(asset_id, message_body, database):

    target_id = message_body["asset_info"]["target_id"]
    src_sys_id = message_body["asset_info"]["src_sys_id"]
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

    asset_nm = message_body["asset_info"]["asset_nm"]

    data_asset = {
        "asset_id": asset_id,
        "src_sys_id": src_sys_id,
        "target_id": target_id,
        "file_header": bool(message_body["asset_info"]["file_header"]),
        "multipartition": bool(message_body["asset_info"]["multipartition"]),
        "file_type": message_body["asset_info"]["file_type"],
        "asset_nm": asset_nm,
        "source_path": f"s3://{bucket_name_source}/{asset_id}/init/",
        "target_path": f"s3://{bucket_name_target}/{subdomain}/{asset_id}/",
        "trigger_file_pattern": message_body["asset_info"]["trigger_file_pattern"],
        "file_delim": message_body["asset_info"]["file_delim"],
        "file_encryption_ind": bool(message_body["asset_info"]["file_encryption_ind"]),
        "athena_table_name": f"{subdomain}_{asset_nm}",
        "asset_owner": message_body["asset_info"]["asset_owner"],
        "support_cntct": message_body["asset_info"]["support_cntct"],
        "rs_load_ind": bool(message_body["asset_info"]["rs_load_ind"]),
        "rs_stg_table_nm": None,
        "modified_ts": datetime.utcnow()
    }

    if data_asset["rs_load_ind"] == True:
        data_asset["rs_stg_table_nm"] = f"{asset_nm}_stg"

    return data_asset


def parse_ingestion_attributes(asset_id, message_body, database):

    src_sys_id = message_body["asset_info"]["src_sys_id"]

    freq = "None"
    if "frequency" in message_body["ingestion_attributes"].keys():
        if message_body["ingestion_attributes"]["frequency"] != "":
            freq = message_body["ingestion_attributes"]["frequency"]

    ingestion_attributes = {
        "asset_id": asset_id,
        "src_sys_id": src_sys_id,
        "src_table_name": None,
        "src_sql_query": None,
        "ingstn_src_path": None,
        "trigger_mechanism": message_body["ingestion_attributes"]["trigger_mechanism"],
        "frequency": freq,
        "modified_ts": datetime.utcnow(),
        "data_stream": None,
        "ext_method": message_body["ingestion_attributes"]["ext_method"],
        "ext_col": message_body["ingestion_attributes"]["ext_col"]
    }

    # Getting required data from source_system_ingstn_atrbts table
    ingestion_pattern = database.retrieve_dict(
        table="source_system_ingstn_atrbts",
        cols="ingstn_pattern",
        where=("src_sys_id=%s", [src_sys_id])
    )[0]["ingstn_pattern"]
    if ingestion_pattern == "file":
        if message_body["ingestion_attributes"]["trigger_mechanism"] == "time_driven":
            ingestion_attributes[
                "ingstn_src_path"
            ] = f"s3://dl-fmwrk-time-drvn-inbound-us-east-2/init/{src_sys_id}/{asset_id}/"
        else:
            ingestion_attributes[
                "ingstn_src_path"
            ] = f"s3://dl-fmwrk-evnt-drvn-inbound-us-east-2/init/{src_sys_id}/{asset_id}/"
    elif ingestion_pattern == "stream":
        ingestion_attributes["data_stream"] = message_body["ingestion_attributes"]["data_stream"]
        data_stream_name = message_body["ingestion_attributes"]["data_stream"]
        create_delivery_stream(data_stream_name, src_sys_id, asset_id)
    else:
        ingestion_attributes["src_table_name"] = message_body["ingestion_attributes"]["src_table_name"]
        ingestion_attributes["src_sql_query"] = message_body["ingestion_attributes"]["src_sql_query"]

    return ingestion_attributes


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
            "data_type": message_body["asset_attributes"][i]["data_type"],
            "datetime_format": message_body["asset_attributes"][i]["datetime_format"],
            "tgt_datetime_format": message_body["asset_attributes"][i]["tgt_datetime_format"]
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
        if message_body["asset_attributes"][i]["req_tokenization"] == True:
            attribute["tgt_data_type"] = "String"
        if attribute["data_type"] == "Datetime":
            if (attribute["datetime_format"] != None) and (attribute["tgt_datetime_format"] != None):
                attribute["datetime_format"] = message_body["asset_attributes"][i]["datetime_format"]
                attribute["tgt_datetime_format"] = message_body["asset_attributes"][i]["tgt_datetime_format"]
            elif attribute["datetime_format"] != None:
                attribute["datetime_format"] = message_body["asset_attributes"][i]["datetime_format"]
                attribute["tgt_datetime_format"] = message_body["asset_attributes"][i]["datetime_format"]
            else:
                attribute["datetime_format"] = "yyyy-MM-dd HH:mm:ss"
                attribute["tgt_datetime_format"] = "yyyy-MM-dd HH:mm:ss"
        else:
            attribute["datetime_format"] = None
            attribute["tgt_datetime_format"] = None

        data_asset_attributes.append(attribute)

    return data_asset_attributes


def create_asset(event, method, config, database):
    payload = event["body-json"]

    # API logic here
    # -----------

    asset_id = generate_asset_id(10)

    data_asset = parse_asset_info(asset_id, payload, database)

    data_asset_attributes = parse_data_asset_attributes(asset_id, payload)

    ingestion_attributes = parse_ingestion_attributes(
        asset_id, payload, database)

    src_sys_id = data_asset["src_sys_id"]
    trigger_mechanism = ingestion_attributes["trigger_mechanism"]
    support_email = data_asset["support_cntct"]
    freq = ingestion_attributes["frequency"]

    # get adv_dq_rules
    adv_dq_rules = parse_adv_dq(
        payload, asset_id, src_sys_id
    )

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
        # creating body without ts
        del data_asset["modified_ts"]
        for i in data_asset_attributes:
            del i["modified_ts"]
        del ingestion_attributes["modified_ts"]
        if adv_dq_rules:
            for i in adv_dq_rules:
                del i["created_ts"]
        body = {
            "data_asset": data_asset,
            "data_asset_attributes": data_asset_attributes,
            "data_asset_ingstn_atrbts": ingestion_attributes,
            "adv_dq_rules": adv_dq_rules
        }

        create_src_s3_dir_str(
            asset_id=asset_id,
            message_body=payload,
            config=config,
            mechanism=trigger_mechanism
        )
        glue_airflow_trigger(
            asset_id=str(asset_id),
            source_id=str(src_sys_id),
            schedule=freq,
            email=support_email
        )
        database.close()
        status = True
    except Exception as e:
        print(e)
        status = False
        body = str(e)
        database.rollback()
        database.close()

    # -----------

    response = Response(
        method=method,
        status=status,
        body=body,
        payload=payload
    )
    return response.get_response()
