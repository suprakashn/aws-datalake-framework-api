import os
from datetime import datetime

import boto3

from utils import rollback_target_sys, insert_event_to_dynamoDb
from api_response import Response


def workgroup_exists(workgroup, region):
    """

    :param workgroup:
    :param region:
    :return:
    """
    exists = False
    client = boto3.client('athena', region_name=region)
    response = client.list_work_groups()
    workgroups = [i['Name'] for i in response['WorkGroups']]
    if workgroup in workgroups:
        exists = True
    return exists


def create_workgroup(name, region, output_location):
    """

    :param name:
    :param region:
    :param output_location:
    :return:
    """
    client = boto3.client('athena', region_name=region)
    client.create_work_group(
        Name=name,
        Configuration={
            'ResultConfiguration': {
                'OutputLocation': output_location
            },
        },
        Description='AWS DL Framework Workgroup',
        Tags=[
            {
                'Key': 'Name',
                'Value': 'AWS DL Framework'
            },
        ]
    )


def create_athena_db(tgt_config, global_config, region):
    """

    :param tgt_config:
    :param global_config:
    :param region:
    :return:
    """
    ath = boto3.client("athena", region_name=region)
    db_name = tgt_config["domain"]
    wg_name = global_config["workgroup"]
    query = f"create database {db_name}"
    query_path = f"s3://{tgt_config['bucket_name']}/athena/query_results/"
    workgroup = wg_name if workgroup_exists(wg_name, region) \
        else create_workgroup(wg_name, region, query_path)
    response = ath.list_databases(
        CatalogName="AwsDataCatalog",
    )
    if db_name in response["DatabaseList"]:
        print(f"Database {db_name} already exists")
    else:
        ath.start_query_execution(
            QueryString=query,
            WorkGroup=workgroup,
        )


def run_cft(global_config, target_id, region):
    """

    :param global_config:
    :param target_id:
    :param region:
    :return:
    """
    target_cft = "cft/targetSystem.yaml"
    with open(target_cft) as yaml_file:
        template_body = yaml_file.read()
    print(
        "Setup target system flow through {}-{}-{} stack".format(
            global_config["fm_tgt_prefix"], str(target_id), region
        )
    )
    stack = boto3.client("cloudformation", region_name=region)
    stack_name = f"{global_config['fm_tgt_prefix']}-{str(target_id)}-{region}"
    stack.create_stack(
        StackName=stack_name,
        TemplateBody=template_body,
        Parameters=[
            {
                "ParameterKey": "CurrentRegion",
                "ParameterValue": region
            },
            {
                "ParameterKey": "DlFmwrkPrefix",
                "ParameterValue": global_config["fm_tgt_prefix"],
            },
            {
                "ParameterKey": "AwsAccount",
                "ParameterValue": os.environ["aws_account"],
            },
            {
                "ParameterKey": "tgtSysId",
                "ParameterValue": str(target_id),
            },
        ],
    )


def redshift_db_exists(rs_conn, db_name):
    """

    :param rs_conn:
    :param db_name:
    :return:
    """
    databases = rs_conn.list_databases()
    if db_name in databases:
        return True
    return False


def redshift_schema_exists(rs_conn, schema_name):
    """

    :param rs_conn:
    :param schema_name:
    :return:
    """
    schemas = rs_conn.list_schemas()
    if schema_name in schemas:
        return True
    return False


def create_redshift_db(target_data, rs_conn):
    """

    :param target_data:
    :param rs_conn:
    :return:
    """
    # check if the DB exists or not
    db_name = target_data['domain']
    if redshift_db_exists(rs_conn, db_name):
        print(f"DB {db_name} exists")
    else:
        rs_conn.create_database(db_name)
    rs_conn.switch_database(db_name)
    return rs_conn


def create_redshift_schema(target_data, rs_conn):
    """

    :param target_data:
    :param rs_conn:
    :return:
    """
    rs_load_ind = target_data['rs_load_ind']
    if rs_load_ind:
        schema = target_data['subdomain']
        if redshift_schema_exists(rs_conn, schema):
            print(f"Schema {schema} exists")
        else:
            print(f"Creating a new Schema: {schema}")
            rs_conn.create_schema(schema)
    else:
        print("Not Creating a Schema in Redshift since load_ind is DISABLED")


def insert_metadata(metadata_conn, target_table, target_data):
    """

    :param metadata_conn:
    :param target_table:
    :param target_data:
    :return:
    """
    rs_load_ind = target_data['rs_load_ind']
    if rs_load_ind:
        target_data['rs_db_nm'] = target_data['domain']
        target_data['rs_schema_nm'] = target_data['subdomain']
    else:
        target_data['rs_db_nm'] = None
        target_data['rs_schema_nm'] = None
    metadata_conn.insert(table=target_table, data=target_data)


def create_target_system(
        method, global_config, region, metadata_conn,
        redshift_conn, source_payload, target_data
):
    """

    :param method:
    :param global_config:
    :param region:
    :param metadata_conn:
    :param redshift_conn:
    :param source_payload:
    :param target_data:
    :return:
    """
    status = False
    message = None
    target_id = target_data['target_id']
    target_sys_table = global_config['target_sys_table']
    try:
        run_cft(global_config, target_id, region)
        redshift_conn = create_redshift_db(target_data, redshift_conn)
        create_redshift_schema(target_data, redshift_conn)
        create_athena_db(target_data, global_config, region)
        insert_metadata(metadata_conn, target_sys_table, target_data)
        status = True
    except Exception as e:
        message = str(e)
        rollback_target_sys(metadata_conn, global_config, target_id, region)
    resp_ob = Response(
        method, status, body=target_data,
        payload=source_payload, message=message
    )
    response = resp_ob.get_response()
    return response
