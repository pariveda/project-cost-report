from project_blue_costs import get_cost_report
import sys
import os


def lambda_handler(event, context):
    tenants_role_arn = os.environ['TENANTS_ROLE_ARN']
    print('TENANTS_ROLE_ARN:', tenants_role_arn)
    cost_report = get_cost_report(tenants_role_arn, region='us-west-2')
    return {
        "statusCode": 200,
        "body": cost_report
    }


if __name__ == '__main__':
    lambda_handler(None, None)
