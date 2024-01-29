import json
import sys
json_to_insert = {'id': '44311ab6', 'job_id': '2024-01-22_23_02_49-12931597370515843515', 'job_status': 'Done', 'job_success_status': 'SUCCESS', 'time_taken': '', 'job_metrics': {'TotalVcpuTime': 2699, 'TotalMemoryUsage': 17965788, 'numberOfRowsRead': 999990, 'numberOfRowDeidentified': 999990}, 'streaming': False, 'load_test_details': ''}


if __name__ == '__main__':
    try:
        project_id = sys.argv[1]
        job_id = sys.argv[2]

        file = open('.github/workflows/scripts/metrics.json', 'w')
        json.dump(json_to_insert, file)

        file.close()

    except Exception as e:
        print(e)
