from datetime import datetime
from api_response import Response


def update_src(db, src_config: dict, global_config: dict):
    exists, message = None, "unable to update"
    non_editable_params = ["src_sys_id", "bucket_name"]
    if src_config:
        src_sys_id = src_config["src_sys_id"]
        data_to_update = src_config["update_data"]
        data_to_update['modified_ts'] = str(datetime.utcnow())
        src_sys_table = global_config["src_sys_table"]
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = True if db.retrieve(src_sys_table, "src_sys_id", condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(src_sys_table, data_to_update, condition)
                message = "updated"
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source system DNE"
    else:
        message = "No update config provided"
    return exists, message


def update_ing(db, ingestion_config: dict, global_config: dict):
    exists, message = None, "unable to update"
    non_editable_params = ["src_sys_id", "ingstn_src_bckt_nm"]
    if ingestion_config:
        src_sys_id = ingestion_config["src_sys_id"]
        data_to_update = ingestion_config["update_data"]
        data_to_update['modified_ts'] = str(datetime.utcnow())
        ingestion_table = global_config["ingestion_table"]
        condition = ("src_sys_id=%s", [src_sys_id])
        exists = (
            True if db.retrieve(ingestion_table, "src_sys_id", condition) else False
        )
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                db.update(ingestion_table, data_to_update, condition)
                message = "updated"
            else:
                message = "Trying to update a non editable parameter: src_sys_id / bucket name"
        else:
            message = "Source System DNE"
    else:
        message = "No update config provided"
    return exists, message


def update_source_system(method, db, message_body, global_config):
    src_config = message_body["src_config"]
    ingestion_config = message_body["ingestion_config"]
    src_sys_id = src_config['src_sys_id']
    src_sys_table = global_config["src_sys_table"]
    ingestion_table = global_config["ingestion_table"]
    src_exists, src_msg = update_src(db, src_config, global_config)
    ing_exists, ing_msg = update_ing(
        db, ingestion_config, global_config
    )
    message = src_msg + ' ' + ing_msg
    if src_exists:
        sql = f"""
            SELECT * from {src_sys_table} A join {ingestion_table} B 
            ON A.src_sys_id = B.src_sys_id
            WHERE A.src_sys_id = {src_sys_id}
            """
        source_system_details = db.execute(sql, return_type='dict')[0]
        source_system_details.pop("modified_ts")
        response = Response(
            method, status=True, body=source_system_details,
            payload=message_body, message=message
        )
    else:
        response = Response(
            method, status=True, body=None,
            payload=message_body, message=message
        )
    return response.get_response()
