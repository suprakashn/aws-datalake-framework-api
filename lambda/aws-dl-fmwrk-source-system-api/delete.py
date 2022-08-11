import boto3
from utils import src_sys_present, is_associated_with_asset
from api_response import Response


def delete_rds_entry(db, global_config, src_sys_id):
    src_sys_id_int = int(src_sys_id)
    src_table = global_config["src_sys_table"]
    src_ingstn_table = global_config['ingestion_table']
    condition = ("src_sys_id = %s", [src_sys_id_int])
    db.delete(src_table, condition)
    db.delete(src_ingstn_table, condition)


def empty_bucket(bucket_name):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.objects.all().delete()


def delete_src_sys_stack(global_config, src_sys_id, region):
    stack_name = global_config["fm_prefix"] + "-" + str(src_sys_id) + "-" + region
    client = boto3.client("cloudformation", region_name=region)
    try:
        # Deletion of stack
        client.delete_stack(StackName=stack_name)
    except Exception as e:
        print(e)


def delete_source_system(method, db, global_config, message_body, region):
    src_config = message_body["src_config"]
    src_sys_id = int(src_config["src_sys_id"])
    src_bucket = f"{global_config['fm_prefix']}-{src_sys_id}-{region}"
    if src_sys_present(db, global_config, src_sys_id):
        associated = is_associated_with_asset(db, src_sys_id)
        # If it is not associated,source system stack will be deleted
        if not associated:
            try:
                # empty the bucket
                empty_bucket(src_bucket)
                # delete the stack
                delete_src_sys_stack(global_config, src_sys_id, region)
                # delete the entry in the RDS
                delete_rds_entry(db, global_config, src_sys_id)
                response = Response(
                    method, status=True, body=None, payload=message_body,
                    message="Successfully Deleted the Source System"
                )
            except Exception as e:
                response = Response(
                    method, status=False, body=None, payload=message_body,
                    message=str(e)
                )
        else:
            response = Response(
                method, status=False, body=None, payload=message_body,
                message="There are assets attached with the source system"
            )
    else:
        response = Response(
            method, status=False, body=None, payload=message_body,
            message="Source System Does Not Exist"
        )
    return response.get_response()
