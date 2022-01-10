from project_blue_costs import get_cost_report
import sys

def lambda_handler(event, context):
    try:
        cost_report = get_cost_report()
        return {
            "statusCode": 200,
            "body": cost_report
        }
    except:
        raise Exception('Unable to retrieve cost report.\n {} \n {}'.format(
            str(sys.exc_info()[0]), str(sys.exc_info()[1])))

if __name__ == '__main__':
    lambda_handler(None, None)