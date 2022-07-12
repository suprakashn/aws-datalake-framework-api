from utils import *

from create import create_asset
from read import read_asset
from update import update_asset
from delete import delete_asset

from api_response import Response


def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]

    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            response = Response(method, True, "body", None)
            return response.get_response()

        elif method == "create":
            db = get_database()
            global_config = getGlobalParams()
            generated_response = create_asset(
                event, context, config=global_config, database=db)
            response = Response(
                method=method,
                status=generated_response["status"],
                body=generated_response["body"],
                payload=generated_response["payload"]
            )
            return response.get_response()

        elif method == "read":
            db = get_database()
            generated_response = read_asset(event, context, database=db)
            response = Response(
                method=method,
                status=generated_response["status"],
                body=generated_response["body"],
                payload=generated_response["payload"]
            )
            return response.get_response()

        elif method == "update":
            db = get_database()
            generated_response = update_asset(event, context, database=db)
            response = Response(
                method=method,
                status=generated_response["status"],
                body=generated_response["body"],
                payload=generated_response["payload"]
            )
            return response.get_response()

        elif method == "delete":
            db = get_database()
            generated_response = delete_asset(event, context, database=db)
            response = Response(
                method=method,
                status=generated_response["status"],
                body=generated_response["body"],
                payload=generated_response["payload"]
            )
            return response.get_response()

        else:
            return {"statusCode": "404", "body": "Not found"}
