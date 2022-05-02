import boto3

class dataAsset:
    def __init__(self):
        client = boto3.resource("dynamodb")
        self.table = client.Table("dl-fmwrk.data_asset")

    def Health(self):
        return {"statusCode": "200", "body": "API Health is good"}
