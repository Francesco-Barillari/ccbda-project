import json
import os
import boto3
import urllib.request
from datetime import datetime, timezone

ALPHA_VANTAGE_API_KEY = os.environ['ALPHAVANTAGEKEY']
S3_BUCKET_NAME = os.environ['S3BUCKET']
ASSETS = json.loads(os.environ.get('ASSETS', '["AAPL"]'))

s3 = boto3.client('s3', region_name=os.environ.get('APPREGION', 'eu-west-1'))


def fetch_price(symbol):
    url = (
        f"https://www.alphavantage.co/query"
        f"?function=GLOBAL_QUOTE"
        f"&symbol={symbol}"
        f"&apikey={ALPHA_VANTAGE_API_KEY}"
    )
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())

    quote = data.get("Global Quote", {})
    if not quote:
        raise ValueError(f"No data returned for {symbol}")

    return {
        "symbol": symbol,
        "price": float(quote["05. price"]),
        "volume": int(quote["06. volume"]),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def save_to_s3(record):
    now = datetime.now(timezone.utc)
    key = f"raw/{record['symbol']}/{now.strftime('%Y/%m/%d/%H%M%S')}.json"
    s3.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=key,
        Body=json.dumps(record).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def lambda_handler(event, context):
    results = []
    for symbol in ASSETS:
        try:
            record = fetch_price(symbol)
            key = save_to_s3(record)
            results.append({"symbol": symbol, "status": "ok", "price": record["price"], "s3_key": key})
            print(f"[OK] {symbol} @ {record['price']} saved to s3://{S3_BUCKET_NAME}/{key}")
        except Exception as e:
            results.append({"symbol": symbol, "status": "error", "message": str(e)})
            print(f"[ERROR] {symbol}: {e}")

    return {"statusCode": 200, "body": json.dumps(results)}
