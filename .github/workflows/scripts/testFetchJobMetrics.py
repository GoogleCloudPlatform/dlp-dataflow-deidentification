import json
import sys
json_to_insert = {'id': '44311ab6', 'job_id': '2024-01-22_23_02_49-12931597370515843515', 'job_status': 'Done', 'job_success_status': 'SUCCESS', 'time_taken': '9 Mins', 'job_metrics': {'TotalVcpuTime': 2699, 'TotalMemoryUsage': 17965788, 'numberOfRowsRead': 999990, 'numberOfRowDeidentified': 999990}, 'streaming': False, 'load_test_details': ''}

def create_job_report_file(job_details,test_report_file):
    # | load test details| dataflow_job_id | job success status | time taken |
    template = "|{0:}|{1:}|{2:}|{3:}|"
    test_report = template.format(job_details["id"],
                                  job_details["job_id"],
                                  job_details["job_success_status"],
                                  job_details["time_taken"])
    print(test_report)
    file = open(test_report_file, "w")
    file.write(test_report)
    file.close()

    return

if __name__ == '__main__':
    try:
        project_id = sys.argv[1]
        job_id = sys.argv[2]
        file_name = sys.argv[3]

        create_job_report_file(json_to_insert,file_name)
        # file = open(file_name, 'w')
        # json.dump(json_to_insert, file)

        # file.close()

    except Exception as e:
        print(e)
