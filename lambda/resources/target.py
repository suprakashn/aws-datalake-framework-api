import boto3

class targetSystem:
    def __init__(self):
        client = boto3.resource("dynamodb")
        self.table = client.Table("dl-fmwrk.target_system")

    def Health(self):
        return {"statusCode": "200", "body": "API Health is good"}
