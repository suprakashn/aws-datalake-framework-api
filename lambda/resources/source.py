import boto3

class sourceSystem:
    def __init__(self):
        client = boto3.resource("dynamodb")
        self.table = client.Table("dl-fmwrk.source_system")

    def Health(self):
        return {"statusCode": "200", "body": "API Health is good"}

    def Create_data(self, event):
        response = self.table.put_item(
            Item={
                "src_sys_id": event["src_sys_id"],
                "bucket_name": event["bucket_name"],
                "src_sys_nm": event["src_sys_nm"],
                "mechanism": event["mechanism"],
                "data_owner": event["data_owner"],
                "support_cntct": event["support_cntct"],
            }
        )
        return {
            "statusCode": response["ResponseMetadata"]["HTTPStatusCode"],
            "body": "Record " + str(event["src_sys_id"]) + " added",
        }

    def Delete_data(self, event):
        response = self.table.delete_item(Key={
            "src_sys_id": event["src_sys_id"],
            "bucket_name": event["bucket_name"]
        })

        return {
            "statusCode": "200",
            "body": "Deleted the item with id :" + str(event["src_sys_id"]),
        }
