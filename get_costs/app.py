import project_costs as pc


def lambda_handler(event, context):
    cost_report = pc.get_cost_report_data()
    pc.post_to_slack(cost_report)

    return {
        "statusCode": 200,
        "body": pc.to_json(cost_report)
    }


if __name__ == '__main__':
    print(lambda_handler(None, None))
