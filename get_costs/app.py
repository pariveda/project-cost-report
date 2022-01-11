import os
from project_blue_costs import get_cost_report, post_to_slack


def lambda_handler(event, context):
    tenants_role_arn = os.environ['TENANTS_ROLE_ARN']
    cost_report = get_cost_report(tenants_role_arn)
    post_to_slack(cost_report)

    return {
        "statusCode": 200,
        "body": cost_report
    }


if __name__ == '__main__':
    lambda_handler(None, None)
