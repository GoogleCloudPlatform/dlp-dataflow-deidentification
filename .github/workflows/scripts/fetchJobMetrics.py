from google.cloud import dataflow_v1beta3
import sys
from google.cloud import bigquery
import uuid

# valid_metrics = [ "numberOfRowDeidentified", "numberOfRowsRead"]

valid_metrics = [ "TotalVcpuTime", "TotalMemoryUsage", "numberOfRowDeidentified", "numberOfRowsRead"]

def fetch_job_metrics(project_id,job_id):
    # Create a client
    client = dataflow_v1beta3.MetricsV1Beta3Client()

    # Initialize request argument(s)
    request = dataflow_v1beta3.GetJobMetricsRequest(
        project_id = project_id,
        job_id=job_id
    )

    # Make the request
    response = client.get_job_metrics(request=request)

    # Handle the response
    return response

def verify_job_success(job_metrics):
    if job_metrics["numberOfRowsRead"] == job_metrics["numberOfRowDeidentified"]:
        return "SUCCESS"
    else :
        return "PARTIAL_SUCCESS"

def write_data_to_bigquery(project_id,job_id,job_success_status,metrics):

    client = bigquery.Client()
    dataset_id = "load_test_report"
    table_id = "load_test_report"
    table_ref = client.dataset(dataset_id,project=project_id).table(table_id)
    table = client.get_table(table_ref)
    row_uuid = str(uuid.uuid4())[:8]
    print(row_uuid)

    row_to_insert = {
        "id":row_uuid ,
        "job_id": job_id,
        "job_status": "Done",
        "job_success_status": job_success_status,
        "time_taken": "",
        "job_metrics": metrics,
        "streaming": False,
        "load_test_details": ""

    }
    print(row_to_insert)

    error = client.insert_rows(table, [row_to_insert])
    print(error)
    return

def prepare_metrics_data(metrics):
    return

if __name__ == '__main__':
    try:
        project_id = sys.argv[1]
        job_id = sys.argv[2]

        job_metrics = fetch_job_metrics(project_id,job_id)

        metrics_map = {}

        for metric in job_metrics.metrics:
            # print(metric.name.name)
            if metric.name.name in valid_metrics:
                metric_name = metric.name.name
                if "tentative" not in metric.name.context:
                    value = int(metric.scalar)
                    if metric_name not in metrics_map:
                        metrics_map[metric_name] = value
                    else:
                        metrics_map[metric_name] = metrics_map[metric_name] + int(value)


        print(metrics_map)

        job_success_status = verify_job_success(metrics_map)

        write_data_to_bigquery(project_id,job_id,job_success_status,metrics_map)

    except Exception as e:
        print(e)