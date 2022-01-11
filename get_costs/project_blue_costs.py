import boto3
import pandas as pd


def main():
    print(get_cost_report())


def get_cost_report(tenants_role, region='us-east-1'):
    monthly_params = dict(
        TimePeriod={
            'Start': '2021-10-01',
            'End': '2022-02-01'
        },
        Granularity='MONTHLY',
        Metrics=[
            'AmortizedCost',
        ]
    )

    weekly_params = dict(
        TimePeriod={
            'Start': '2022-01-01',
            'End': '2022-02-01'
        },
        Granularity='DAILY',
        Metrics=[
            'AmortizedCost',
        ]
    )

    report = f"""
# Monthly Costs #
{get_cost_table(monthly_params, tenants_role, region)}
# Weekly Costs #
{get_cost_table(weekly_params, tenants_role, region)}
    """

    return report


def get_cost_table(params: dict, tenants_role, region) -> pd.DataFrame:
    assumed_creds = None
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

    df_central = pd.DataFrame(flatten('PB Central', central_costs))
    df_tenants = pd.DataFrame(flatten('PB Tenants', tenants_costs))

    df_combined = df_central.merge(
        df_tenants,
        on='StartDate',
        how='left').set_index('StartDate')

    if params['Granularity'] == 'DAILY':
        df_combined = df_combined.resample('W', label='left').sum()

    df_combined['PB Central ($)'] = df_combined['PB Central ($)'].apply(
        lambda x: "${:>5,}".format(int(x)))
    df_combined['PB Tenants ($)'] = df_combined['PB Tenants ($)'].apply(
        lambda x: "${:>5,}".format(int(x)))

    return df_combined


def flatten(name: str, results: pd.DataFrame) -> pd.DataFrame:
    flattened = pd.DataFrame(map(lambda x: {
        'StartDate': x['TimePeriod']['Start'],
        f'{name} ($)': float(x['Total']['AmortizedCost']['Amount'])
    }, results))
    flattened['StartDate'] = pd.to_datetime(flattened['StartDate'])
    return flattened


if __name__ == '__main__':
    main()
