from utils import *

from api_response import Response


def read_asset(event, method, database):
    message_body = event["body-json"]

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
        "tgt_data_type",
        "datetime_format",
        "tgt_datetime_format"
    ]
    ingestion_columns = [
        "asset_id",
        "src_sys_id",
        "src_table_name",
        "src_sql_query",
        "ingstn_src_path",
        "trigger_mechanism",
        "frequency",
        "ext_method",
        "ext_col"
    ]
    dq_columns = [
        'dq_rule'
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
        )
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
            )
            dict_dq = database.retrieve_dict(
                table="adv_dq_rules",
                cols=dq_columns,
                where=("asset_id=%s", [asset_id])
            )
            status = True
            body = {
                "asset_info": dict_asset[0],
                "asset_attributes": dict_attributes,
                "ingestion_attributes": dict_ingestion[0] if dict_ingestion else {},
                "adv_dq_rules": [i["dq_rule"] for i in dict_dq if dict_dq]
            }
        else:
            status = False
            body = {}
        database.close()

    except Exception as e:
        database.close()
        print(e)
        status = False
        body = str(e)

    # -----------
    response = Response(
        method=method,
        status=status,
        body=body,
        payload=message_body
    )
    return response.get_response()
