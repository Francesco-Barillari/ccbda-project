"""
Creates all AWS resources for Phase 2:
  - SQS queue (price data buffer)
  - Glue Python Shell job (data preparation → Forecast-ready CSVs)
  - Lambda: glue-trigger (EventBridge → starts Glue job)
  - EventBridge rule (triggers glue-trigger every hour)

After running, config/pipeline.json is auto-updated with the queue URL and bucket.
Re-run at any time to redeploy code changes (Glue script, processors, config).

To send test messages locally: python scripts/send_test_messages.py --help
"""

import io
import json
import os
import zipfile

import boto3
from botocore.exceptions import ClientError
from dotenv import dotenv_values

ROOT_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
config    = dotenv_values(os.path.join(ROOT_DIR, ".env"))

REGION    = config["AWS_DEFAULT_REGION"]
TEAM      = config["TEAM"]
S3_BUCKET = config["S3_BUCKET_NAME"]

QUEUE_NAME      = f"{TEAM}-price-data-queue"
GLUE_JOB_NAME   = f"{TEAM}-data-preparation"
LAMBDA_TRIGGER  = f"{TEAM}-glue-trigger"
RULE_NAME       = f"{TEAM}-prepare-every-hour"
GLUE_ROLE_NAME  = f"{TEAM}-glue-role"

s3_client     = boto3.client("s3",     region_name=REGION)
sqs_client    = boto3.client("sqs",    region_name=REGION)
glue_client   = boto3.client("glue",   region_name=REGION)
iam_client    = boto3.client("iam")
lmb_client    = boto3.client("lambda", region_name=REGION)
events_client = boto3.client("events", region_name=REGION)


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------

def create_sqs_queue() -> str:
    print(f"Creating SQS queue: {QUEUE_NAME}")
    resp = sqs_client.create_queue(
        QueueName=QUEUE_NAME,
        Attributes={
            "VisibilityTimeout":             "300",   # 5 min — covers Glue processing window
            "MessageRetentionPeriod":        "86400", # 1 day
            "ReceiveMessageWaitTimeSeconds": "1",
        },
    )
    url = resp["QueueUrl"]
    print(f"  [OK] {url}")
    return url


# ---------------------------------------------------------------------------
# Config upload
# ---------------------------------------------------------------------------

def update_and_upload_config(queue_url: str) -> None:
    pipeline_path = os.path.join(ROOT_DIR, "config", "pipeline.json")
    assets_path   = os.path.join(ROOT_DIR, "config", "assets.json")

    with open(pipeline_path) as f:
        pipeline = json.load(f)

    pipeline["s3"]["bucket"] = S3_BUCKET
    for source in pipeline["sources"]:
        if source["name"] == "alpha_vantage_price":
            source["queue_url"] = queue_url

    with open(pipeline_path, "w") as f:
        json.dump(pipeline, f, indent=2)
    print("Updated config/pipeline.json with queue URL and bucket")

    for local_path, s3_key in [
        (pipeline_path, "config/pipeline.json"),
        (assets_path,   "config/assets.json"),
    ]:
        s3_client.upload_file(local_path, S3_BUCKET, s3_key)
        print(f"  [OK] Uploaded {s3_key}")


# ---------------------------------------------------------------------------
# Glue IAM role
# ---------------------------------------------------------------------------

def get_glue_role() -> str:
    override = config.get("GLUE_ROLE_ARN", "").strip()
    if override:
        print(f"  Using GLUE_ROLE_ARN from .env: {override}")
        return override

    for candidate in [GLUE_ROLE_NAME, "AWSGlueServiceRole", "LabRole"]:
        try:
            arn = iam_client.get_role(RoleName=candidate)["Role"]["Arn"]
            print(f"  [OK] Using existing IAM role: {candidate}")
            return arn
        except iam_client.exceptions.NoSuchEntityException:
            pass

    print(f"Creating Glue IAM role: {GLUE_ROLE_NAME}")
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"}],
    }
    role = iam_client.create_role(RoleName=GLUE_ROLE_NAME, AssumeRolePolicyDocument=json.dumps(trust))
    for policy in [
        "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    ]:
        iam_client.attach_role_policy(RoleName=GLUE_ROLE_NAME, PolicyArn=policy)
    print(f"  [OK] Created: {role['Role']['Arn']}")
    return role["Role"]["Arn"]


# ---------------------------------------------------------------------------
# Glue scripts
# ---------------------------------------------------------------------------

def upload_glue_scripts() -> tuple:
    script_key = "glue-scripts/data_preparation/job.py"
    s3_client.upload_file(
        os.path.join(ROOT_DIR, "glue", "data_preparation", "job.py"),
        S3_BUCKET, script_key,
    )
    print(f"  [OK] Uploaded job.py → s3://{S3_BUCKET}/{script_key}")

    buf      = io.BytesIO()
    proc_dir = os.path.join(ROOT_DIR, "glue", "data_preparation", "processors")
    base_dir = os.path.join(ROOT_DIR, "glue", "data_preparation")
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(proc_dir):
            for fname in files:
                filepath = os.path.join(root, fname)
                zf.write(filepath, os.path.relpath(filepath, base_dir))

    zip_key = "glue-scripts/data_preparation/processors.zip"
    buf.seek(0)
    s3_client.put_object(Bucket=S3_BUCKET, Key=zip_key, Body=buf.getvalue())
    print(f"  [OK] Uploaded processors.zip → s3://{S3_BUCKET}/{zip_key}")
    return script_key, zip_key


# ---------------------------------------------------------------------------
# Glue job
# ---------------------------------------------------------------------------

def create_glue_job(role_arn: str, script_key: str, zip_key: str) -> None:
    print(f"Creating Glue job: {GLUE_JOB_NAME}")
    job_config = {
        "Role": role_arn,
        "Command": {
            "Name":           "pythonshell",
            "ScriptLocation": f"s3://{S3_BUCKET}/{script_key}",
            "PythonVersion":  "3.9",
        },
        "DefaultArguments": {
            "--config_s3_bucket":   S3_BUCKET,
            "--config_s3_key":      "config/pipeline.json",
            "--assets_s3_key":      "config/assets.json",
            "--processors_zip_key": zip_key,
            "--TempDir":            f"s3://{S3_BUCKET}/glue-temp/",
        },
        "MaxCapacity": 0.0625,  # minimum Python Shell DPU (1/16)
        "Timeout":     30,
        "GlueVersion": "3.0",
    }

    try:
        glue_client.create_job(Name=GLUE_JOB_NAME, **job_config)
        print(f"  [OK] Job created")
    except (glue_client.exceptions.AlreadyExistsException, ClientError) as e:
        if isinstance(e, ClientError) and e.response["Error"]["Code"] not in (
            "AlreadyExistsException", "IdempotentParameterMismatchException"
        ):
            raise
        glue_client.update_job(JobName=GLUE_JOB_NAME, JobUpdate=job_config)
        print(f"  [OK] Job updated")


# ---------------------------------------------------------------------------
# Lambda: glue-trigger
# ---------------------------------------------------------------------------

def get_lambda_role() -> str:
    override = config.get("LAMBDA_ROLE_ARN", "").strip()
    if override:
        return override
    arn = iam_client.get_role(RoleName="lambda-function-role-python")["Role"]["Arn"]
    print(f"  [OK] Lambda role: {arn}")
    return arn


def grant_lambda_glue_permission(lambda_role_arn: str) -> None:
    role_name   = lambda_role_arn.split("/")[-1]
    policy_name = f"{TEAM}-glue-start-job"
    policy_doc  = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect":   "Allow",
            "Action":   "glue:StartJobRun",
            "Resource": f"arn:aws:glue:{REGION}:{lambda_role_arn.split(':')[4]}:job/{GLUE_JOB_NAME}",
        }],
    })
    iam_client.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=policy_doc,
    )
    print(f"  [OK] Granted glue:StartJobRun on {GLUE_JOB_NAME} to {role_name}")


def deploy_glue_trigger(role_arn: str) -> str:
    print(f"Deploying Lambda: {LAMBDA_TRIGGER}")
    src_dir = os.path.join(ROOT_DIR, "lambda", "glue_trigger")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for fname in os.listdir(src_dir):
            fpath = os.path.join(src_dir, fname)
            if os.path.isfile(fpath):
                zf.write(fpath, fname)
    code = buf.getvalue()

    env_vars = {"GLUE_JOB_NAME": GLUE_JOB_NAME}

    try:
        lmb_client.update_function_code(FunctionName=LAMBDA_TRIGGER, ZipFile=code)
        lmb_client.get_waiter("function_updated").wait(FunctionName=LAMBDA_TRIGGER)
        lmb_client.update_function_configuration(
            FunctionName=LAMBDA_TRIGGER, Environment={"Variables": env_vars}
        )
        arn = lmb_client.get_function(FunctionName=LAMBDA_TRIGGER)["Configuration"]["FunctionArn"]
        print(f"  [OK] Updated: {arn}")
        return arn
    except lmb_client.exceptions.ResourceNotFoundException:
        pass

    resp = lmb_client.create_function(
        FunctionName=LAMBDA_TRIGGER,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": code},
        Timeout=30,
        Environment={"Variables": env_vars},
        Description="Started by EventBridge hourly to kick off the Glue data-preparation job",
    )
    print(f"  [OK] Created: {resp['FunctionArn']}")
    return resp["FunctionArn"]


# ---------------------------------------------------------------------------
# EventBridge
# ---------------------------------------------------------------------------

def create_eventbridge_rule(trigger_arn: str) -> None:
    print(f"Creating EventBridge rule: {RULE_NAME}")
    try:
        rule = events_client.put_rule(
            Name=RULE_NAME,
            ScheduleExpression="rate(1 hour)",
            State="ENABLED",
        )
        try:
            lmb_client.add_permission(
                FunctionName=trigger_arn,
                StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=rule["RuleArn"],
            )
        except lmb_client.exceptions.ResourceConflictException:
            pass
        events_client.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": trigger_arn}],
        )
        print(f"  [OK] Rule triggers {LAMBDA_TRIGGER} every hour")

    except ClientError as e:
        if e.response["Error"]["Code"] != "AccessDeniedException":
            raise
        print(f"  [SKIP] No permission to create EventBridge rule — create it manually:")
        print(f"    AWS Console → EventBridge → Rules → Create rule")
        print(f"    Name:     {RULE_NAME}")
        print(f"    Schedule: rate(1 hour)")
        print(f"    Target:   Lambda → {LAMBDA_TRIGGER}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=== Phase 2 Setup: SQS + Glue + EventBridge ===\n")

    queue_url = create_sqs_queue()
    update_and_upload_config(queue_url)

    print("\nSetting up Glue...")
    glue_role_arn        = get_glue_role()
    script_key, zip_key  = upload_glue_scripts()
    create_glue_job(glue_role_arn, script_key, zip_key)

    print("\nSetting up Lambda + EventBridge...")
    lambda_role_arn = get_lambda_role()
    grant_lambda_glue_permission(lambda_role_arn)
    trigger_arn     = deploy_glue_trigger(lambda_role_arn)
    create_eventbridge_rule(trigger_arn)

    print("\n=== Phase 2 setup complete ===")
    print(f"\nAlso add SQS_QUEUE_URL={queue_url} to your .env for local testing")
    print(f"\nTo test:")
    print(f"  python scripts/send_test_messages.py --symbols AAPL --messages 12")
    print(f"  aws lambda invoke --function-name {LAMBDA_TRIGGER} --payload '{{}}' /dev/null")
    print(f"  aws s3 ls s3://{S3_BUCKET}/forecast-ready/ --recursive")
