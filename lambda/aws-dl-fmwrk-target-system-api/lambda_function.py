import boto3
import os
import json
from datetime import datetime

def insert_event_to_dynamoDb(event, context, api_call_type, status="success", op_type="insert"):
  cur_time = datetime.now()
  aws_request_id = context.aws_request_id
  log_group_name = context.log_group_name
  log_stream_name = context.log_stream_name
  function_name = context.function_name
  method_name = event["context"]["resource-path"]
  query_string = event["params"]["querystring"]
  payload = event["body-json"]

  client = boto3.resource("dynamodb")
  table = client.Table("aws-dl-fmwrk-api-events")

  if op_type == "insert":
      response = table.put_item(
          Item={
              "aws_request_id": aws_request_id,
              "method_name": method_name,
              "log_group_name": log_group_name,
              "log_stream_name": log_stream_name,
              "function_name": function_name,
              "query_string": query_string,
              "payload": payload,
              "api_call_type": api_call_type,
              "modified ts": str(cur_time),
              "status": status,
          })
  else:
      response = table.update_item(
        Key={
          'aws_request_id': aws_request_id,
          'method_name': method_name,
        },
        ConditionExpression="attribute_exists(aws_request_id)",
        UpdateExpression='SET status = :val1',
        ExpressionAttributeValues = {
          ':val1': status,
        }
      )

  if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
    body = "Insert/Update of the event with aws_request_id=" + aws_request_id + " completed successfully"
  else:
    body = "Insert/Update of the event with aws_request_id=" + aws_request_id + " failed"

  return {
      "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
      "body": body,
  }

def create_target(event, context):
  message_body = event["body-json"]
  api_call_type = "synchronous"


  # API logic here
  # -----------

  # -----------

  # API event entry in dynamoDb
  response = insert_event_to_dynamoDb(event, context, api_call_type)
  return{
      "statusCode": "200",
      "sourcePayload": message_body,
      "sourceCodeDynamoDb": response["statusCode"],
      "body": "create_target function to be defined",
  }

def read_target(event, context):
  message_body = event["body-json"]
  api_call_type = "synchronous"


  # API logic here
  # -----------

  # -----------

  # API event entry in dynamoDb
  response = insert_event_to_dynamoDb(event, context, api_call_type)
  return{
      "statusCode": "200",
      "sourcePayload": message_body,
      "sourceCodeDynamoDb": response["statusCode"],
      "body": "read_target function to be defined",
  }

def update_target(event, context):
  message_body = event["body-json"]
  api_call_type = "synchronous"


  # API logic here
  # -----------

  # -----------

  # API event entry in dynamoDb
  response = insert_event_to_dynamoDb(event, context, api_call_type)
  return{
      "statusCode": "200",
      "sourcePayload": message_body,
      "sourceCodeDynamoDb": response["statusCode"],
      "body": "update_target function to be defined",
  }

def delete_target(event, context):
  message_body = event["body-json"]
  api_call_type = "synchronous"


  # API logic here
  # -----------

  # -----------

  # API event entry in dynamoDb
  response = insert_event_to_dynamoDb(event, context, api_call_type)
  return{
      "statusCode": "200",
      "sourcePayload": message_body,
      "sourceCodeDynamoDb": response["statusCode"],
      "body": "delete_target function to be defined",
  }

def lambda_handler(event, context):
    resource = event["context"]["resource-path"][1:]
    taskType = resource.split("/")[0]
    method = resource.split("/")[1]
    
    print(event)
    print(taskType)
    print(method)

    if event:
        if method == "health":
            return {"statusCode": "200", "body": "API Health is good"}
            
        elif method == "create":
            response = create_target(event, context)
            return response
            
        elif method == "read":
            response = read_target(event, context)
            return response
            
        elif method == "update":
            response = update_target(event, context)
            return response
            
        elif method == "delete":
            response = delete_target(event, context)
            return response
            
        else:
            return {"statusCode": "404", "body": "Not found"}
