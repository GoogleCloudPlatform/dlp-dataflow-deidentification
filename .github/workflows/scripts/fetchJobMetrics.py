import datetime

from google.cloud import dataflow_v1beta3
import sys
from google.cloud import bigquery
import uuid
import json
from google.cloud import monitoring_v3
import time

VALID_METRICS = ["TotalVcpuTime", "TotalMemoryUsage", "numberOfRowDeidentified", "numberOfRowsRead"]
TABLE_NAME = "load_test_metrics"
BQ_DATASET_ID = "load_test_report"

class LoadTest:
    def __init__(self, test_id, project_id, job_id, test_details, test_name):
        self.test_uuid = test_id
        self.project_id = project_id
        self.job_id = job_id
        self.test_details = test_details
        self.test_name = test_name
        self.dataflow_job_details = self.set_job_details()

    def set_job_details(self):
        client = dataflow_v1beta3.JobsV1Beta3Client()
        request = dataflow_v1beta3.GetJobRequest(project_id=self.project_id,
                                             job_id=self.job_id)
        response = client.get_job(request=request)
        return response

    def fetch_metrics(self):
        client = dataflow_v1beta3.MetricsV1Beta3Client()
        request = dataflow_v1beta3.GetJobMetricsRequest(
            project_id=self.project_id,
            job_id=self.job_id
        )
        response = client.get_job_metrics(request=request)
        return response

    def get_job_metrics(self):
        job_metrics = self.fetch_metrics()

        metrics_map = {}

        for metric in job_metrics.metrics:
            # print(metric.name.name)
            if metric.name.name in VALID_METRICS:
                metric_name = metric.name.name
                if "tentative" not in metric.name.context:
                    value = int(metric.scalar)
                    if metric_name not in metrics_map:
                        metrics_map[metric_name] = value
                    else:
                        metrics_map[metric_name] = metrics_map[metric_name] + int(value)

        print(metrics_map)
        return metrics_map

    def get_elapsed_time(self):
        client = monitoring_v3.MetricServiceClient()
        project_name = f"projects/{self.project_id}"

        series = monitoring_v3.TimeSeries()
        series.metric.type = "dataflow.googleapis.com/job/elapsed_time"
        series.metric.labels["job_id"] = self.job_id

        now = time.time()
        seconds = int(now)
        nanos = int((now - seconds) * 10 ** 9)
        interval = monitoring_v3.TimeInterval(
            {"end_time": {"seconds": seconds, "nanos": nanos},
             "start_time": self.dataflow_job_details.create_time}
        )
        print("Fetching elapased time...")
        # Initialize request argument(s)
        request = monitoring_v3.ListTimeSeriesRequest(
            name=project_name,
            filter="metric.type=\"dataflow.googleapis.com/job/elapsed_time\" AND metric.labels.job_id=\"{}\"".format(self.job_id),
            view="FULL",
            interval=interval
            # Add aggregation parametes
            # https://github.com/GoogleCloudPlatform/dataflow-metrics-exporter/blob/main/src/main/java/com/google/cloud/dfmetrics/pipelinemanager/MonitoringClient.java#L140
        )
        page_result = client.list_time_series(request=request)
        elapsed_time = 0
        for response in page_result:
            for point in response.points:
                # print(point.value.int64_value)
                elapsed_time = max(elapsed_time, point.value.int64_value)

        return elapsed_time

    def get_job_success_status(self,job_metrics):
        if job_metrics["numberOfRowsRead"] == job_metrics["numberOfRowDeidentified"]:
            return "SUCCESS"
        elif job_metrics["numberOfRowDeidentified"] == 0:
            return "FAILURE"
        else:
            return "PARTIAL_SUCCESS"


    def write_data_to_bigquery(self,row):
        client = bigquery.Client()
        table_ref = client.dataset(BQ_DATASET_ID, project=project_id).table(TABLE_NAME)
        table = client.get_table(table_ref)
        print(row)
        error = client.insert_rows(table, [row])
        print(error)

    def get_job_type(self):
        if self.dataflow_job_details.type_ == dataflow_v1beta3.types.JobType.JOB_TYPE_BATCH.name:
            return "BATCH"
        elif self.dataflow_job_details.type_ == dataflow_v1beta3.types.JobType.JOB_TYPE_STREAMING:
            return "STREAMING"
        return "NONE"


    def get_monitoring_dashboard(self):
        # client = monitoring_dashboard_v1.DashboardsServiceClient()
        # request = monitoring_dashboard_v1.GetDashboardRequest(
        #     name="projects/175500764928/dashboards/f291443e-c268-4b79-8875-b3258a66bea4",
        # )    response = client.get_dashboard(request=request)
        # print(response)
        print(type(self.dataflow_job_details.create_time),self.dataflow_job_details.create_time)
        print(self.dataflow_job_details.create_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        print("Current time", datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        start_time = self.dataflow_job_details.create_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        end_time = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        monitoring_dashboard_url = "https://pantheon.corp.google.com/monitoring/dashboards/builder/9f527992-6420-4717" \
                                   "-a888-a9bc76aa6a33;startTime={};endTime={}?project=dlp-dataflow-load-test".format(
            start_time, end_time
        )

        print(monitoring_dashboard_url)
        return monitoring_dashboard_url


    def prepare_metrics_data(self, metrics):

        test_run_data = {
            "test_id": self.test_uuid,
            "test_name": self.test_name,
            "dataflow_job_id": self.job_id,
            "job_type": self.dataflow_job_details.type_.name,
            "dataflow_job_state": str(self.dataflow_job_details.current_state.name),
            "job_success_status": self.get_job_success_status(metrics),
            "time_taken": self.get_elapsed_time(),
            "job_metrics": metrics,
            "load_test_details": json.dumps(self.test_details),
            "file_type": self.test_details["file_type"],
            "file_size": self.test_details["file_size"],
            "timestamp": datetime.datetime.utcnow(),
            "monitoring_dashboard": self.get_monitoring_dashboard()
        }
        return test_run_data


if __name__ == '__main__':
    try:
        project_id = sys.argv[1]
        job_id = sys.argv[2]
        test_id = sys.argv[3]
        test_name = sys.argv[4]
        test_details = json.loads(sys.argv[5])

        test_job_object = LoadTest(
            test_id, project_id, job_id, test_details, test_name
        )

        job_metrics = test_job_object.get_job_metrics()

        test_job_object.write_data_to_bigquery(test_job_object.prepare_metrics_data(job_metrics))

    except Exception as e:
        print(e)
