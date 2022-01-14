import os
from datetime import datetime
from typing import Dict

import boto3
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

TENANTS_ROLE_ARN = os.environ['TENANTS_ROLE_ARN']
COLS_TO_KEEP = ['TimePeriod.Start', 'Total.AmortizedCost.Amount']
CENTRAL_COL_HEADER = 'Central'
TENANTS_COL_HEADER = 'Tenants'

SLACK_POST_TEMPLATE = """
:money_with_wings:  :aws:  :money_with_wings:

*Rolling 5 Months*
```
{}
```

*Rolling 6 Weeks*
```
{}
```
"""


def post_to_slack(report_data: object) -> None:
    # Get Slack webhook URL from secrets manager.
    webhook_url_secret = os.environ['SLACK_WEBHOOK_URL_SECRET']
    print('webhook_url_secret_id:', webhook_url_secret)
    secrets_client = boto3.client('secretsmanager')
    webhook_url = secrets_client.get_secret_value(
        SecretId=webhook_url_secret)['SecretString']

    # Format for slack post.
    report = SLACK_POST_TEMPLATE.format(
        report_data['monthly_cost_table'], report_data['weekly_cost_table'])

    requests.post(webhook_url, json={'text': report})


def to_json(report_data: Dict[str, pd.DataFrame]):
    return {
        k: str(v) for k, v in report_data.items()
    }


def get_cost_report_data() -> Dict[str, pd.DataFrame]:
    today = datetime.today()
    monthly_start_date = (today - relativedelta(months=4)
                          ).replace(day=1).date().isoformat()
    weekly_start_date = (today - relativedelta(weeks=5)
                         ).date().isoformat()

    return dict(
        monthly_cost_table=get_cost_table(
            TimePeriod={
                'Start': monthly_start_date,
                'End': today.date().isoformat()
            },
            Granularity='MONTHLY',
            Metrics=[
                'AmortizedCost',
            ]
        ),
        weekly_cost_table=get_cost_table(
            TimePeriod={
                'Start': weekly_start_date,
                'End': today.date().isoformat()
            },
            Granularity='DAILY',
            Metrics=[
                'AmortizedCost',
            ]
        )
    )


def get_cost_table(**params) -> pd.DataFrame:
    # Get cost data from AWS Cost Explorer service.
    sts_client = boto3.client('sts')
    assumed_creds = sts_client.assume_role(
        RoleArn=TENANTS_ROLE_ARN,
        RoleSessionName='GetProjectBlueCosts'
    )['Credentials']

    pb_central_client = boto3.client('ce')
    pb_tenants_client = boto3.client(
        'ce',
        aws_access_key_id=assumed_creds['AccessKeyId'],
        aws_secret_access_key=assumed_creds['SecretAccessKey'],
        aws_session_token=assumed_creds['SessionToken'])

    central_costs = pb_central_client.get_cost_and_usage(
        **params)['ResultsByTime']
    tenants_costs = pb_tenants_client.get_cost_and_usage(
        **params)['ResultsByTime']

    # Flatten and trim results.
    df_central = pd.json_normalize(central_costs)[COLS_TO_KEEP]
    df_tenants = pd.json_normalize(tenants_costs)[COLS_TO_KEEP]

    # Update column headers.
    df_central.columns = ['StartDate', CENTRAL_COL_HEADER]
    df_tenants.columns = ['StartDate', TENANTS_COL_HEADER]

    # Join the results from both accounts.
    df_combined = df_central.merge(
        df_tenants,
        on='StartDate',
        how='outer').set_index('StartDate')

    # Convert to appropriate data types.
    df_combined.index = pd.to_datetime(df_combined.index)
    df_combined = df_combined.apply(lambda _: _.astype(float).astype(int))

    # Cost Explorer service does not support Weekly granularity, so we need to resample from Daily.
    if params['Granularity'] == 'DAILY':
        df_combined = df_combined.resample('W', label='left').sum()

    # Format currency columns for report.
    df_combined = df_combined.applymap("${:>5,}".format)

    return df_combined
