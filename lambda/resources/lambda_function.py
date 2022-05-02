import boto3
import os
import json
from source import sourceSystem
from target import targetSystem
from asset import dataAsset

def lambda_handler(event, context):
    taskType = event["params"]["querystring"]["tasktype"]
    method = event["context"]["resource-path"][1:]

    if event:
        
        # Source System Operations
        if method == "sourcesystem":
            sourceSystem_instance = sourceSystem()

            # Check the health of sourceSystem Method
            if taskType == "health":
                health_result = sourceSystem_instance.Health()
                return health_result

            # Create a new sourceSystem
            elif taskType == "create":
                create_result = sourceSystem_instance.Create_data(event["body-json"])
                return create_result

            # Delete an existing sourceSystem
            elif taskType == "delete":
                delete_result = sourceSystem_instance.Delete_data(event["body-json"])
                return delete_result

            # Error Condition
            else:
                return {"statusCode": "404", "body": "Not found"}

        # Target System Operations
        elif method == "targetsystem":
            targetSystem_instance = targetSystem()

            # Check the health of sourceSystem Method
            if taskType == "health":
                health_result = targetSystem_instance.Health()
                return health_result

        # Data Asset Operations
        elif method == "dataasset":
            dataAsset_instance = dataAsset()

            # Check the health of sourceSystem Method
            if taskType == "health":
                health_result = dataAsset_instance.Health()
                return health_result

        else:
            return {"statusCode": "404", "body": "Not found"}
