from api_response import Response


def read_source_system(method, db, message_body, global_config):
    fetch_limit = message_body["fetch_limit"]
    src_config = message_body["src_config"]
    src_sys_table = global_config["src_sys_table"]
    ingestion_table = global_config["ingestion_table"]
    source_system_details = None
    default_sql = f"""
            SELECT A.src_sys_id,A.bucket_name,A.src_sys_nm,A.src_sys_desc,
            A.mechanism,A.data_owner,A.support_cntct,
            B.ingstn_pattern,B.db_type,B.db_hostname,B.db_username,
            B.db_schema,B.db_port,B.ingstn_src_bckt_nm,B.db_name
            FROM {src_sys_table} A
            LEFT JOIN {ingestion_table} B
            ON A.src_sys_id=B.src_sys_id
            """
    if fetch_limit in [None, "None", "0", "NONE", "null"] and src_config:
        src_sys_id = src_config["src_sys_id"]
        if isinstance(src_sys_id, list):
            src_sys_ids = ','.join(map(str, src_sys_id)).rstrip(',')
            sql = default_sql + f" WHERE A.src_sys_id in ({src_sys_ids})"
            source_system_details = db.execute(sql, return_type='dict')
        else:
            sql = default_sql + f" WHERE A.src_sys_id={int(src_sys_id)}"
            source_system_details = db.execute(sql, return_type='dict')
    # if a fetch limit exists then fetch all the cols of limited src_systems
    elif isinstance(fetch_limit, int) or fetch_limit.isdigit():
        limit = int(fetch_limit)
        sql = default_sql + f" LIMIT {limit}"
        source_system_details = db.execute(sql, return_type='dict')
    # if neither of the above case satisfies fetch all the info
    elif fetch_limit == "all":
        sql = default_sql
        source_system_details = db.execute(sql, return_type='dict')
    if source_system_details:
        response = Response(
            method, status=True, body=source_system_details,
            payload=message_body
        )
    else:
        response = Response(
            method, status=False, body=source_system_details,
            payload=message_body, message='Source system Does Not Exist'
        )
    return response.get_response()
