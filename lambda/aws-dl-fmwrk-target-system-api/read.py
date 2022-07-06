from api_response import Response


def read_target_system(
    method, metadata_db, fetch_limit,
        source_payload, global_config
    ):
    """

    :param method:
    :param metadata_db:
    :param fetch_limit:
    :param source_payload:
    :param global_config:
    :return:
    """
    target_info = None
    cols = ['target_id', 'domain', 'subdomain',
            'bucket_name', 'data_owner', 'support_cntct']
    table = global_config['target_sys_table']
    if fetch_limit in [None, 'None', '0', 'NONE'] and source_payload:
        target_id = int(source_payload['target_id'])
        condition = ("target_id=%s", [target_id])
        target_info = metadata_db.retrieve_dict(table=table, cols=cols, where=condition)
    # if a fetch limit exists then fetch all the cols of limited target_systems
    elif isinstance(fetch_limit, int) or fetch_limit.isdigit():
        limit = int(fetch_limit)
        target_info = metadata_db.retrieve_dict(table=table, cols=cols, limit=limit)
    # if neither of the above case satisfies fetch all the info
    elif fetch_limit == 'all':
        target_info = metadata_db.retrieve_dict(table=table, cols=cols)
    if target_info:
        status = True
        resp = Response(
            method,
            status,
            target_info,
            source_payload
        )
    else:
        status = False
        resp = Response(
            method,
            status,
            target_info,
            source_payload,
            message='The information about the resource DNE'
        )
    return resp

