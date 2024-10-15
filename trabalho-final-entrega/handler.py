import json
import boto3

EM_PREPARO_URL = "https://sqs.us-east-1.amazonaws.com/301080176247/em-preparo-pizzaria"
PRONTO_URL = "https://sqs.us-east-1.amazonaws.com/301080176247/pronto-pizzaria"
DYNAMO_TABLE = "pedidos-pizzaria"

sqs = boto3.client("sqs")
dynamodb = boto3.resource("dynamodb")

def send_to_queue(event, context):
    for record in event["Records"]:
        key = record["s3"]["object"]["key"]
        bucket = record["s3"]["bucket"]["name"]

        if key.startswith("em-preparacao/"):
            queue_url = EM_PREPARO_URL
        elif key.startswith("pronto/"):
            queue_url = PRONTO_URL
        else:
            return
    
        msg = {"bucket": bucket, "key": key}
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(msg)
        )

def insert_into_dynamodb(event, context):
    table = dynamodb.Table(DYNAMO_TABLE)

    for record in event["Records"]:
        body = json.loads(record["body"])

        item = {
            "pedido": body["key"].split("/")[-1],
            "datetime": record["attributes"]["SentTimestamp"]
        }

        table.put_item(Item=item)