from utils import *

from api_response import Response


def delete_dag(asset_id, src_sys_id):
    print(f"deleting dag for asset_id : {asset_id}")
    dag = f"/mnt/dags/{src_sys_id}_{asset_id}_workflow.py"
    print(f"dag name : {dag}")
    if os.path.exists(dag):
        os.remove(dag)


def delete_asset(event, method, database):
    message_body = event["body-json"]

    # API logic here
    # -----------

    asset_id = message_body["asset_id"]
    src_sys_id = message_body["src_sys_id"]

    try:
        print(f"deleting catalogs for asset_id : {asset_id}")
        database.delete(
            table="data_asset_catalogs",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
        print(f"deleting adv dq rules for asset_id : {asset_id}")
        database.delete(
            table="adv_dq_rules",
            where=("asset_id=%s", [asset_id])
        )
        print(f"deleting asset attributes for asset_id : {asset_id}")
        database.delete(
            table="data_asset_attributes",
            where=("asset_id=%s", [asset_id])
        )
        print(f"deleting ingestion attributes for asset_id : {asset_id}")
        database.delete(
            table="data_asset_ingstn_atrbts",
            where=(
                "asset_id=%s and src_sys_id=%s",
                [asset_id, src_sys_id]
            )
        )
        print(f"deleting asset info for asset_id : {asset_id}")
        database.delete(
            table="data_asset",
            where=("asset_id=%s", [asset_id])
        )
        print(
            f"retrieving ingestion pattern for asset_id : {asset_id} from source_system_ingstn_atrbts")
        ingestion_pattern = database.retrieve_dict(
            table="source_system_ingstn_atrbts",
            cols="ingstn_pattern",
            where=("src_sys_id=%s", [src_sys_id])
        )[0]["ingstn_pattern"]
        if ingestion_pattern == "stream":
            print(f"deleting delivery stream for asset_id : {asset_id}")
            delete_delivery_stream(src_sys_id, asset_id)
        delete_dag(asset_id, src_sys_id)
        database.close()
        status = True
        body = f"deleted_asset : {asset_id}"
        print(body)

    except Exception as e:
        print(e)
        status = False
        body = str(e)
        database.rollback()
        print("rolled back the changes made in database")
        database.close()

    # -----------

    response = Response(
        method=method,
        status=status,
        body=body,
        payload=message_body
    )
    return response.get_response()
