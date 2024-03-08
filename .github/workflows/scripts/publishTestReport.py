import sys, os
from google.cloud import bigquery

TEST_REPORT_TEMPLATE_FILE = '.github/workflows/test_report_template.md'
TABLE_NAME = "load_test_metrics"
BQ_DATASET_ID = "load_test_report"

def get_job_details(job_details):
    template = "|{0:}|{1:}|{2:}|{3:}|\n"
    test_report = template.format(job_details["test_name"],
                                  job_details["file_type"],
                                  job_details["dataflow_job_state"],
                                  job_details["dataflow_job_id"])
    print(test_report)
    return test_report


def fetch_test_details(project_id, test_id):
    client = bigquery.Client()
    table_ref = client.dataset(BQ_DATASET_ID, project=project_id).table(TABLE_NAME)
    table = client.get_table(table_ref)
    query = """
        SELECT *
        FROM `{0}.{1}.{2}`
        WHERE test_id = '{3}'
    """.format(project_id, BQ_DATASET_ID, TABLE_NAME, test_id)
    print(query)
    rows = client.query_and_wait(query)

    return rows


if __name__ == '__main__':
    project_id = sys.argv[1]
    test_id = sys.argv[2]

    # Fetch the results from BigQuery
    rows = fetch_test_details(project_id, test_id)

    # create summary using the report template
    test_details = ""
    for row in rows:
        test_details = test_details + get_job_details(row)

    template_file = open(TEST_REPORT_TEMPLATE_FILE, 'r')
    template = template_file.read()

    job_report = template.format(test_id, test_details)
    template_file.close()

    # Append the summary to GITHUB_STEP_SUMMARY
    f = open(os.environ["GITHUB_STEP_SUMMARY"], "a")
    print(job_report, file=f)
