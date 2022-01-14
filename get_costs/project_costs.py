from datetime import datetime
import os

import boto3
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from dateutil import tz
import numpy as np

CENTRAL_COL_HEADER = 'Central'
TENANTS_COL_HEADER = 'Tenants'
COLS_TO_KEEP = ['TimePeriod.Start', 'Total.AmortizedCost.Amount']

def post_to_slack(report: str):
    webhook_url_secret = os.environ['SLACK_WEBHOOK_URL_SECRET']
    print('webhook_url_secret_id:', webhook_url_secret)
    secrets_client = boto3.client('secretsmanager')
    webhook_url = secrets_client.get_secret_value(
        SecretId=webhook_url_secret)['SecretString']
    data = {'text': report}
    requests.post(webhook_url, json=data)


def get_cost_report():
    today = datetime.today()
    monthly_start_date = (today - relativedelta(months=4)
                          ).replace(day=1).date().isoformat()
    weekly_start_date = (today - relativedelta(weeks=5)
                         ).date().isoformat()

    monthly_params = dict(
        TimePeriod={
            'Start': monthly_start_date,
            'End': today.date().isoformat()
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost',
        ]
    )

    weekly_params = dict(
        TimePeriod={
            'Start': weekly_start_date,
            'End': today.date().isoformat()
        },
        Granularity='DAILY',
        Metrics=[
            'AmortizedCost',
        ]
    )

    report = f"""
:money_with_wings:  :aws:  :money_with_wings:

*Rolling 5 Months*
```
{get_cost_table(monthly_params)}
```

*Rolling 6 Weeks*
```
{get_cost_table(weekly_params)}
```
    """
    # _Generated_: `{datetime.now().astimezone(tz.gettz('Central')).strftime("%Y-%m-%d %H:%M:%S %Z")}`

    return report


def get_cost_table(params: dict) -> pd.DataFrame:
    # Get cost data from AWS Cost Explorer service.
    sts_client = boto3.client('sts')
    tenant_role_arn = os.environ['TENANTS_ROLE_ARN']
    assumed_creds = sts_client.assume_role(
        RoleArn=tenant_role_arn,
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
    