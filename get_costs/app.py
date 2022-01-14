import os
from project_costs import get_cost_report, post_to_slack


def lambda_handler(event, context):
    cost_report = get_cost_report()
    post_to_slack(cost_report)

    return {
        "statusCode": 200,
        "body": cost_report
    }


if __name__ == '__main__':
    lambda_handler(None, None)
