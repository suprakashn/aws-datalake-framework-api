from utils import *

from api_response import Response


def delete_asset(event, method, database):
    message_body = event["body-json"]

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]

    try:
        database.delete(
            table="data_asset_catalogs",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
        database.delete(
            table="adv_dq_rules",
            where=("asset_id=%s", [asset_id])
        )
        database.delete(
            table="data_asset_attributes",
            where=("asset_id=%s", [asset_id])
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
            where=("asset_id=%s", [asset_id])
        )
        ingestion_pattern = database.retrieve_dict(
        table="source_system_ingstn_atrbts",
        cols="ingstn_pattern",
        where=("src_sys_id=%s", [src_sys_id])
        )[0]["ingstn_pattern"]
        if ingestion_pattern=="stream":
            delete_delivery_stream(src_sys_id,asset_id)
        dag = f"/mnt/dags/{src_sys_id}_{asset_id}_workflow.py"
        if os.path.exists(dag):
            os.remove(dag)
        database.close()
        status = True
        body = f"deleted_asset : {asset_id}"

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
        payload=message_body
    )
    return response.get_response()
