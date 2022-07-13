import boto3
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
        database.close()
        status = True
        status_code = 204

        if status == 204:
            bucket_name = "dl-fmwrk-mwaa-us-east-2"
            file_name = f"dags/{src_sys_id}_{asset_id}_workflow.py"
            client = boto3.client('s3')
            client.delete_object(
                Bucket=bucket_name,
                Key=file_name
            )
        body = f"deleted_asset : {asset_id}"

    except Exception as e:
        print(e)
        status = False
        status_code = 404
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
