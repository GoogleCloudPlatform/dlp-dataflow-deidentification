import sys,os
# from google.cloud import bigquery

test_report_template_file = '.github/workflows/load_test_report_template.md'
# test_report_template_file = 'load_test_report_template.md'


json_to_insert = {'id': '44311ab6', 'job_id': '2024-01-22_23_02_49-12931597370515843515', 'job_status': 'Done', 'job_success_status': 'SUCCESS', 'time_taken': '9 Mins', 'job_metrics': {'TotalVcpuTime': 2699, 'TotalMemoryUsage': 17965788, 'numberOfRowsRead': 999990, 'numberOfRowDeidentified': 999990}, 'streaming': False, 'load_test_details': ''}
json_to_insert2 = {'id': '44311ab6', 'job_id': '2024-01-23_23_02_49-12931597370515843515', 'job_status': 'Done', 'job_success_status': 'SUCCESS', 'time_taken': '9 Mins', 'job_metrics': {'TotalVcpuTime': 2699, 'TotalMemoryUsage': 17965788, 'numberOfRowsRead': 999990, 'numberOfRowDeidentified': 999990}, 'streaming': False, 'load_test_details': ''}

def create_job_report_file(job_details):
    # | load test details| dataflow_job_id | job success status | time taken |
    # test_report = template.format(job_details["test_id"],
    #                               job_details["dataflow_job_id"],
    #                               job_details["job_success_status"],
    #                               job_details["time_taken"])
    template = "|{0:}|{1:}|{2:}|{3:}|\n"
    test_report = template.format(job_details["id"],
                                  job_details["job_id"],
                                  job_details["job_success_status"],
                                  job_details["time_taken"])
    print(test_report)
    return test_report

if __name__ == '__main__':
    project_id = sys.argv[1]
    test_id = sys.argv[2]

    #Fetch the results from BigQuery

    # client = bigquery.Client()
    # dataset_id = "load_test_report"
    # table_id = "load_test_report"
    # table_ref = client.dataset(dataset_id,project=project_id).table(table_id)
    # table = client.get_table(table_ref)
    # query = """
    #     SELECT *
    #     FROM `{0}.{1}.{2}`
    #     WHERE test_id = '{3}'
    # """.format(project_id,dataset_id,table_id,test_id)
    # print(query)
    # rows = client.query_and_wait(query)  # Make an API request.

    print("The query data:")

    # for row in rows:
    #     # Row values can be accessed by field name or index.
    #     print(create_job_report_file(row), file=f)

    test_details = create_job_report_file(json_to_insert)
    test_details = test_details + create_job_report_file(json_to_insert2)


    template_file = open(test_report_template_file, 'r')
    template = template_file.read()

    job_report = template.format(test_id,test_details)
    template_file.close()

    # file_name = os.environ["GITHUB_STEP_SUMMARY"]
    # if sys.argc == 4:

    f = open(os.environ["GITHUB_STEP_SUMMARY"], "a")
    print(job_report, file=f)

