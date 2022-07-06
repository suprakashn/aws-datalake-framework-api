import boto3
from api_response import Response


def get_domain(db, table, tgt_id):
    """

    :param db:
    :param table:
    :param tgt_id:
    :return:
    """
    condition = ("target_id=%s", [tgt_id])
    details = db.retrieve_dict(table, cols=['domain'], where=condition)
    db_name = details[0]['domain']
    return db_name


def rollback_target_sys(db, global_config, target_id, region):
    """

    :param db:
    :param global_config:
    :param target_id:
    :param region:
    :return:
    """
    try:
        db.rollback()
        delete_target_sys_stack(global_config, target_id, region)
        delete_rds_entry(db, global_config, target_id)
    except Exception as e:
        print(e)


def is_associated_with_asset(db, target_id):
    """

    :param db:
    :param target_id:
    :return:
    """
    try:
        # Accessing data asset table
        table = 'data_asset'
        condition = ('target_id = %s', [target_id])
        # Trying to get dynamoDB item with target_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='target_id', where=condition
        )
        if response:
            return True
        else:
            return False
    except Exception as e:
        print(e)


def target_present(db, global_config, target_id):
    """

    :param db:
    :param global_config:
    :param target_id:
    :return:
    """
    try:
        table = global_config['target_sys_table']
        condition = ('target_id = %s', [target_id])
        # Trying to get dynamoDB item with target_id and bucket name as key
        response = db.retrieve_dict(
            table, cols='target_id', where=condition
        )
        return response
    except Exception as e:
        print(e)
        return False


def delete_target_sys_stack(global_config, target_id, region):
    """

    :param global_config:
    :param target_id:
    :param region:
    :return:
    """
    stack_name = global_config["fm_tgt_prefix"] + "-" + str(target_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def delete_rds_entry(db, global_config, tgt_id):
    """

    :param db:
    :param global_config:
    :param tgt_id:
    :return:
    """
    target_id = int(tgt_id)
    table = global_config['target_sys_table']
    condition = ('target_id = %s', [target_id])
    db.delete(table, condition)


def delete_database(db, tgt_id, global_config, region):
    """

    :param db:
    :param tgt_id:
    :param global_config:
    :param region:
    :return:
    """
    ath = boto3.client("athena", region_name=region)
    wg_name = global_config["workgroup"]
    table = global_config["target_sys_table"]
    db_name = get_domain(db, table, tgt_id)
    query = f"drop database {db_name}"
    workgroup = wg_name
    response = ath.list_databases(
        CatalogName="AwsDataCatalog",
    )
    print(f"Attempting to delete {db_name}")
    ath.start_query_execution(
        QueryString=query,
        WorkGroup=workgroup,
    )


def delete_redshift(redshift_conn, schema):
    """

    :param redshift_conn:
    :param schema:
    :return:
    """
    # verify schema exists
    if redshift_conn.verify_schema_exists(schema):
        # check if tables are present in the schema
        tables = redshift_conn.list_tables_in_schema(schema)
        # If no tables are present drop the schema
        # If tables are present then do nothing
        if tables:
            return
        else:
            redshift_conn.delete_schema(schema)
    else:
        print("Schema doesn't exist")


def delete_target_system(
        method, metadata_conn, redshift_conn, global_config,
        target_id, region, source_payload
):
    """

    :param method:
    :param metadata_conn:
    :param redshift_conn:
    :param global_config:
    :param target_id:
    :param region:
    :param source_payload:
    :return:
    """
    status = False
    target_info = target_present(metadata_conn, global_config, target_id)
    if target_info:
        associated = is_associated_with_asset(metadata_conn, target_id)
        if not associated:
            try:
                status = True
                schema = target_info['rs_schema_nm']
                delete_rds_entry(metadata_conn, global_config, target_id)
                delete_target_sys_stack(global_config, target_id, region)
                delete_redshift(redshift_conn, schema)
                resp = Response(
                    method, status, body=None, payload=source_payload
                )
                return resp
            except Exception as e:
                status = False
                resp = Response(
                    method, status, body=None,
                    payload=source_payload, message=e
                )
                return resp
        else:
            message = "Asset(s) are associated with the target system"
    else:
        message = "Target system is not present"
    resp = Response(
        method, status, body=None,
        payload=source_payload, message=message
    )
    return resp
