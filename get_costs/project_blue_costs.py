import datetime as dt
import os

import boto3
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta

CENTRAL_COL_HEADER = 'Central'
TENANTS_COL_HEADER = 'Tenants'

def post_to_slack(report: str):
    webhook_url = os.environ['SLACK_WEBHOOK_URL']
    data = { 'text': report }
    requests.post(webhook_url, json=data)


def get_cost_report(tenants_role, region='us-east-1'):
    today = dt.datetime.today()
    monthly_start_date = (today - relativedelta(months=4)).replace(day=1).date().isoformat()
    weekly_start_date = (today - relativedelta(weeks=5)).replace(day=1).date().isoformat()
    
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

*Rolling 5 Months*
```
{get_cost_table(monthly_params, tenants_role, region)}
```

*Rolling 6 Weeks*
```
{get_cost_table(weekly_params, tenants_role, region)}
```
    """

    return report


def get_cost_table(params: dict, tenants_role, region) -> pd.DataFrame:
    sts_client = boto3.client('sts')
    assumed_creds = sts_client.assume_role(
        RoleArn=tenants_role,
        RoleSessionName='GetProjectBlueCosts'
    )['Credentials']

    pb_central_client = boto3.client('ce', region_name=region)
    pb_tenants_client = boto3.client(
        'ce', region_name=region,
        aws_access_key_id=assumed_creds['AccessKeyId'],
        aws_secret_access_key=assumed_creds['SecretAccessKey'],
        aws_session_token=assumed_creds['SessionToken'])

    central_costs = pb_central_client.get_cost_and_usage(
        **params)['ResultsByTime']
    tenants_costs = pb_tenants_client.get_cost_and_usage(
        **params)['ResultsByTime']

    df_central = pd.DataFrame(flatten(CENTRAL_COL_HEADER, central_costs))
    df_tenants = pd.DataFrame(flatten(TENANTS_COL_HEADER, tenants_costs))

    df_combined = df_central.merge(
        df_tenants,
        on='StartDate',
        how='left').set_index('StartDate')

    if params['Granularity'] == 'DAILY':
        df_combined = df_combined.resample('W', label='left').sum()

    df_combined[CENTRAL_COL_HEADER] = df_combined[CENTRAL_COL_HEADER].apply(
        lambda x: "${:>5,}".format(int(x)))
    df_combined[TENANTS_COL_HEADER] = df_combined[TENANTS_COL_HEADER].apply(
        lambda x: "${:>5,}".format(int(x)))

    return df_combined


def flatten(name: str, results: pd.DataFrame) -> pd.DataFrame:
    flattened = pd.DataFrame(map(lambda x: {
        'StartDate': x['TimePeriod']['Start'],
        f'{name}': float(x['Total']['AmortizedCost']['Amount'])
    }, results))
    flattened['StartDate'] = pd.to_datetime(flattened['StartDate'])
    return flattened

