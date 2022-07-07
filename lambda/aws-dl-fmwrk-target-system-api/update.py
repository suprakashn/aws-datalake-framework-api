from datetime import datetime
from api_response import Response
from api_exceptions import NonEditableParams


def update_target_system(method, db, target_config: dict, global_config: dict):
    """

    :param method:
    :param db:
    :param target_config:
    :param global_config:
    :return:
    """
    status = False
    exists, message = None, None
    non_editable_params = ['target_id', 'bucket_name']
    if target_config:
        target_id = target_config['target_id']
        data_to_update = target_config['update_data']
        data_to_update['modified_ts'] = str(datetime.utcnow())
        target_table = global_config['target_sys_table']
        condition = ("target_id=%s", [target_id])
        exists = True if db.retrieve(target_table, 'target_id', condition) else False
        if exists:
            if not any(x in data_to_update.keys() for x in non_editable_params):
                try:
                    db.update(target_table, data_to_update, condition)
                    status = True
                    message = 'updated'
                    resp_ob = Response(
                        method, status, body=data_to_update,
                        payload=target_config, message=message
                    )
                except Exception as e:
                    resp_ob = Response(
                        method, False, body=data_to_update,
                        payload=target_config, message=str(e)
                    )
            else:
                message = "Trying to update a non editable parameter: target_id / bucket name"
                resp_ob = Response(
                    method, status, body=None,
                    payload=target_config, message=message
                )
        else:
            message = "Target system DNE"
            resp_ob = Response(
                method, status, body=None,
                payload=target_config, message=message
            )
    else:
        message = 'No update config provided'
        resp_ob = Response(
            method, status, body=None,
            payload=target_config, message=message
        )
    response = resp_ob.get_response()
    return response
