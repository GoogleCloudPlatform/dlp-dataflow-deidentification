
# Inspect, De-identify and Re-identify sensitive data using Cloud DLP and Dataflow

> This repo contains a reference implementation of an end to end data tokenization solution. The solution is designed to migrate sensitive data to BigQuery after passing it through inspection/de-identification/re-identification Dataflow pipelines implemented using Cloud DLP. Please check out the links below for reference guides.

## Table of Contents

* [Architecture](#architecture)

* [Concepts](#concepts)

* [Tutorial](#tutorial)

	* [Pre-requisites](#pre-requisites)
    * [Compile the code](#compile-the-code)
    * [Run the samples](#run-the-samples)
      * [Inspection](#inspection)
      * [De-identification](#de-identification)
      * [Re-identification](#re-identification-from-bigquery)
    * [Pipeline Parameters](#pipeline-parameters)
    * [Supported File Formats](#supported-file-formats)
    * [S3 Scanner](#s3-scanner)

* [How to adapt this pipeline for your use cases](#how-to-adapt-this-pipeline-for-your-use-cases)

* [Troubleshooting](#troubleshooting)

* [Advanced Topics](#advanced-topics)

* [Disclaimer](#disclaimer)


## Architecture
The solution comprises two types of pipelines (based on the DLP Transformation type). Refer [Dataflow DAG](#dataflow-dag) for pipeline job graph:
1. Inspect and Deidentify
2. ReIdentify

### Inspection and De-identification
![Reference Architecture](diagrams/inspect-deid-architecture.png)

The pipeline can be used for CSV, TSV, Avro, JSONL files stored in either GCS or AWS S3 bucket. It uses State and Timer API for efficient batching to process the files in optimal manner.
The results of inspection/the deidentified data is written to BigQuery table. 

### Re-identification
![Reference Architecture](diagrams/reid-architecture.png)

The pipeline for reidentification workflow is used to read data from BigQuery table and publish the re-identified data in a secure pub sub topic.
## Concepts

1. [Cloud Data Loss Prevention - Quick Start & Guides](https://cloud.google.com/dlp/docs/dlp-bigquery)
2. [De-identification and re-identification of PII in large-scale datasets using Cloud DLP](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp).
3. [Create & Manage Cloud DLP Configurations](https://cloud.google.com/dlp/docs/creating-job-triggers).
4. [Automated Dataflow Pipeline to De-identify PII Dataset](https://cloud.google.com/dataflow/docs/guides/templates/provided/dlp-text-to-bigquery).
5. [Validate Dataset in BigQuery and Re-identify using Dataflow](https://cloud.google.com/solutions/validating-de-identified-data-bigquery-re-identifying-pii-data).
6. [Inspecting storage and databases for sensitive data](https://cloud.google.com/dlp/docs/inspecting-storage)
7. [Dataflow Pipeline Options](https://cloud.google.com/dataflow/docs/reference/pipeline-options)
8. [Cloud DLP Quotas and Limits](https://cloud.google.com/dlp/limits)


## Tutorial

### Pre-requisites

1. Create a new project on Google Cloud Platform.

2. Use the link below to open Google Cloud Shell.

   [![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/GoogleCloudPlatform/dlp-dataflow-deidentification.git)

3. Run the following commands to set up the data tokenization solution in your GCP project. 

```
gcloud config set project <project_id>
sh setup-data-tokeninzation-solution-v2.sh
```

Script (setup-data-tokenization-solution-v2.sh) handles following topics:

* Create a service account for running the DLP pipeline (creates a custom role).

* Emit a set_env.sh that can be used to set temporary environment variables while triggering the DLP pipelines.

* Create a bucket ({project-id}-demo-data) in us-central1 and [uploads a sample dataset](http://storage.googleapis.com/dataflow-dlp-solution-sample-data/sample_data_scripts.tar.gz) with <b>mock</b> PII data.

* Create a BigQuery dataset in US (demo_dataset) to store the tokenized data.

* Create a [KMS wrapped key(KEK)](https://cloud.google.com/kms/docs/envelope-encryption) by creating an automatic [TEK](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp#token_encryption_keys) (Token Encryption Key).

* Create DLP [inspect, de-identification and re-identification templates](https://cloud.google.com/solutions/creating-cloud-dlp-de-identification-transformation-templates-pii-dataset#creating_the_cloud_dlp_templates) with the KEK and crypto based transformations identified in this [section of the guide](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp#determining_transformation_type)

4. Run set_env.sh
```
source set_env.sh
```


### Compile the code

```
gradle spotlessApply

gradle build
```
### Run the samples
#### Inspection

```
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 \
-Pargs=" --region=${REGION} \
--project=${PROJECT_ID} \
--streaming --enableStreamingEngine \
--tempLocation=gs://${PROJECT_ID}-demo-data/temp \
--numWorkers=1 --maxNumWorkers=2 \
--runner=DataflowRunner \
--filePattern=gs://${PROJECT_ID}-demo-data/*.csv \
--dataset=${BQ_DATASET_NAME}   \
--inspectTemplateName=${INSPECT_TEMPLATE_NAME} \
--deidentifyTemplateName=${DEID_TEMPLATE_NAME} \
--DLPMethod=INSPECT" 
```

#### De-Identification

```
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 \
-Pargs=" --region=${REGION} \
--project=${PROJECT_ID} \
--streaming --enableStreamingEngine \
--tempLocation=gs://${PROJECT_ID}-demo-data/temp \
--numWorkers=1 --maxNumWorkers=2 \
--runner=DataflowRunner \
--filePattern=gs://${PROJECT_ID}-demo-data/*.csv \
--dataset=${BQ_DATASET_NAME}   \
--inspectTemplateName=${INSPECT_TEMPLATE_NAME} \
--deidentifyTemplateName=${DEID_TEMPLATE_NAME} \
--DLPMethod=DEID" 
```
You can run some quick [validations](https://cloud.google.com/solutions/validating-de-identified-data-bigquery-re-identifying-pii-data#validating_the_de-identified_dataset_in_bigquery) in BigQuery table to check on tokenized data.


#### Re-Identification From BigQuery
1. Export the Standard SQL Query to read data from bigQuery. One example from our solution guide:
```
export QUERY="select ID,Card_Number,Card_Holders_Name from \`${PROJECT_ID}.${BQ_DATASET_NAME}.CCRecords_1564602828\` where safe_cast(Credit_Limit as int64)>100000 and safe_cast (Age as int64)>50 group by ID,Card_Number,Card_Holders_Name limit 10"
```
2. Create a gcs file with the query:

```
export REID_QUERY_BUCKET=<name>
cat << EOF | gsutil cp - gs://${REID_QUERY_BUCKET}/reid_query.sql
${QUERY}
EOF
```
3. Create a pub-sub topic. Refer [create a topic](https://cloud.google.com/pubsub/docs/create-topic#create_a_topic)

4. Run the pipeline by passing required parameters:
```
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 
-Pargs="--region=<region> 
--project=<project_id>
--streaming --enableStreamingEngine 
--tempLocation=gs://<bucket>/temp 
--numWorkers=5 --maxNumWorkers=10 
--runner=DataflowRunner 
--tableRef=<project_id>:<dataset>.<table> 
--dataset=<dataset> 
--topic=projects/<project_id>/topics/<name> 
--autoscalingAlgorithm=THROUGHPUT_BASED 
--workerMachineType=n1-highmem-4 
--deidentifyTemplateName=projects/<project_id>/deidentifyTemplates/<name> 
--DLPMethod=REID 
--keyRange=1024 
--queryPath=gs://${REID_QUERY_BUCKET}/reid_query.sql"

```

### Pipeline Parameters

| Pipeline Option                  | Description                                                                                                                                                                                                                                                        | Used in Operations  |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| `region`                         | Specifies a regional endpoint for deploying your Dataflow jobs.                                                                                                                                                                                                    | All                 |
| `project`                        | The project ID for your Google Cloud project.                                                                                                                                                                                                                      | All                 |
| `streaming`                      | true is streaming pipeline                                                                                                                                                                                                                                         | INSPECT/DEID        |
| `enableStreamingEngine`          | Specifies whether Dataflow Streaming Engine is enabled or disabled                                                                                                                                                                                                 | INSPECT/DEID        |
| `tempLocation`                   | Cloud Storage path for temporary files. Must be a valid Cloud Storage URL                                                                                                                                                                                          | All                 |
| `numWorkers`                     | (Optional) The initial number of Compute Engine instances to use when executing your pipeline. This option determines how many workers the Dataflow service starts up when your job begins.                                                                        | All                 |
| `maxNumWorkers`                  | (Optional) The maximum number of Compute Engine instances to be made available to your pipeline during execution. This value can be higher than the initial number of workers (specified by numWorkers) to allow your job to scale up, automatically or otherwise. | All                 |
| `runner`                         | DataflowRunner                                                                                                                                                                                                                                                     | All                 |
| `inspectTemplateName`            | DLP Inspect Template Name                                                                                                                                                                                                                                          | INSPECT/DEID        | 
| `deidentifyTemplateName`         | DLP DeIdentify Template Name                                                                                                                                                                                                                                       | All                 |
| `DLPMethod`                      | Type DLP operation to perform - INSPECT/DEID/REID                                                                                                                                                                                                                  | All                 |
| `batchSize`                      | (Optional) Batch size for DLP API, default is 500K                                                                                                                                                                                                                 | All                 |
| `dataset`                        | BQ Dataset to write the inspect/deid results or to read from in case of reid                                                                                                                                                                                       | All                 |
| `recordDelimiter`                | (Optional) Record delimiter                                                                                                                                                                                                                                        | INSPECT/DEID        |
| `columnDelimiter`                | Column Delimiter - Only required in case of custom delimiter                                                                                                                                                                                                       | INSPECT/DEID        | 
| `tableRef`                       | BigQuery table to export from in the form `<project>:<dataset>.<table>`                                                                                                                                                                                            | REID                |
| `queryPath`                      | Query file for reid                                                                                                                                                                                                                                                | REID                |
| `headers`                        | DLP Table Headers- Required for Jsonl file type                                                                                                                                                                                                                    | INSPECT/DEID        |
| `numShardsPerDLPRequestBatching` | (Optional) Number of shards for DLP request batches.Can be used to controls parallelism of DLP requests. Default value is 100                                                                                                                                      | All                 |
| `numberOfWorkerHarnessThreads`   | (Optional) The number of threads per each worker harness process                                                                                                                                                                                                   | All                 |
| `dlpApiRetryCount`               | (Optional) Number of retries in case of transient errors in DLP API, Default value is 10                                                                                                                                                                           | All                 |
| `getInitialBackoff`              | (Optional) Initial backoff (in seconds) for retries with exponential backoff, default is 5s                                                                                                                                                                        | All                 |

Please refer [Pipeline Options](https://cloud.google.com/dataflow/docs/reference/pipeline-options) for more details

### Supported File Formats

1. CSV

The sample commands for processing csv files have been provided in the above section [Build and Run](#run-the-samples)

2. TSV

TSV files are handled in the same way as CSV files with TAB as column delimiter. No additional changes are required in pipeline options.
3. JSONL

The pipeline supports JSONL file format where each line is a valid JSON Object and newline character is used to separate JSON objects. A sample file can be found in [test resources](src/test/resources/CCRecords_sample.jsonl). 
To run the pipeline for JSONL files, the list of comma separated headers also needs to be passed in the pipeline options. 
```
// Copy the sample jsonl file to GCS
gsutil cp ./src/test/resources/CCRecords_sample.jsonl gs://<bucket>/

// Run the pipeline using following command
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 -Pargs=" --region=<region> --project=<projct_id> --streaming --enableStreamingEngine --tempLocation=gs://<bucket>/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --filePattern=gs://<path>.jsonl --dataset=<name>   --inspectTemplateName=<inspect_template> --deidentifyTemplateName=<deid_tmplate> --DLPMethod=DEID --headers=<comma_separated_list_of_headers>"
```
4. Avro

Avro files are handled in the same way as CSV files. No additional changes are required to run the pipeline.

Refer [here](src/test/resources/avro) for sample data.

5. CSV files with custom delimiter 

It is possible to provide csv files with custom delimiter. The delimiter has to be passed in the pipeline option as "--columnDelimiter". 
```
gradle build ... -Pargs="... --columnDelimiter=|"
```



### S3 Scanner

To use AWS S3 as a source of input files, use AWS credentials as instructed below.

Export the AWS access key, secret key, and credentials provider to environment variables. 

```
export AWS_ACCESS_KEY="<access_key>"
export AWS_SECRET_KEY="<secret_key>"
export AWS_CRED="{\"@type\":\"AWSStaticCredentialsProvider\",\"awsAccessKeyId\":\"${AWS_ACCESS_KEY}\",\"awsSecretKey\":\"${AWS_SECRET_KEY}\"}"
```

Use Gradle to build and run the job to performs data loss prevention (DLP) on a CSV file stored in Amazon S3. The results will be written to BigQuery.

```
gradle spotlessApply

gradle build

// inspect is default as DLP Method; For deid: --DLPMethod=DEID
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 -Pargs="--region=<region> --project=<project_id> --streaming --enableStreamingEngine --tempLocation=gs://<bucket>/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --filePattern=s3://<bucket>>/file.csv --dataset=<name>  --inspectTemplateName=<inspect_template> --deidentifyTemplateName=<deid_tmplate> --awsRegion=<aws_region> --awsCredentialsProvider=$AWS_CRED"
```

#### Parameters:

* --awsRegion: The region where the AWS resources reside.

* --awsCredentialsProvider: The AWS credentials provider.


## How to adapt this pipeline for your use cases

1. [Create your own inspection templates & run inspection on sample data](https://cloud.google.com/dlp/docs/creating-templates-inspect)
2. [Create de-identification templates and run de-identification on sample data](https://cloud.google.com/dlp/docs/creating-templates-deid)
3. In case of cross-regional data source or sink, dataflow service account should be granted necessary permissions.


## Troubleshooting

Following are the known issues with Cloud DLP, along with ways you can avoid or recover from them.

1. Duplicate Rows: When writing data to a BigQuery table, Cloud DLP might write duplicate rows.

    > The project uses Streaming Inserts API of BigQuery which by default enables best-effort deduplication mechanism but it should not be relied upon as a mechanism to guarantee the absence of duplicates in your data.
    For solution, checkout [high number of duplicates in Dataflow pipeline streaming inserts to BigQuery](https://cloud.google.com/knowledge/kb/high-number-of-duplicates-in-dataflow-pipeline-streaming-inserts-to-bigquery-000004276?authuser=0).

2. Error in DLP API : "Too many findings in request"

   > DLP has a max findings per request [limit](https://cloud.google.com/dlp/limits#content-redaction-limits) of 3000.
   Run pipeline again with smaller batch size. 

3. DLP API request quota exhausted

   > This can happen if the dataflow pipeline is being run with a small batch size. Rerun the pipeline with a larger value of batch size.
   If batch size cannot be increased or issue persists despite saturating the batch size:
   > 
   >  * Increase the value of parameter dlpApiRetryCount
   >
   >  * The dlp-dataflow-deidentification repo offers a parameter numShardsPerDLPRequestBatching. Reducing this below the default (100) will reduce the number of parallel requests sent to DLP.
   >
   >  * Review if there are other pipelines/clients generating DLP API requests.  Submit a request to increase the quota.
   >
   >  * Submit a request to increase the quota.


## Dataflow DAG

For Deid and Inspect:

![v2_dag_](diagrams/dlp_dag_new.png)



For Reid:

![v2_dag_](diagrams/dlp_reid_dag.png)



## Advanced topics

You can use the gcloud command to trigger the pipeline using Dataflow flex template. Below is an example for de-identification transform from a S3 bucket.

```
gcloud beta dataflow flex-template run "dlp-s3-scanner-deid-demo" --project=<project_id> \
--region=<region> --template-file-gcs-location=gs://dataflow-dlp-solution-sample-data/dynamic_template_dlp_v2.json \
--parameters=^~^streaming=true~enableStreamingEngine=true~tempLocation=gs://<path>/temp~numWorkers=5~maxNumWorkers=5~runner=DataflowRunner~filePattern=<s3orgcspath>/filename.csv~dataset=<bq_dataset>~autoscalingAlgorithm=THROUGHPUT_BASED~workerMachineType=n1-highmem-8~inspectTemplateName=<inspect_template>~deidentifyTemplateName=<deid_template>~awsRegion=ca-central-1~awsCredentialsProvider=$AWS_CRED~batchSize=100000~DLPMethod=DEID

```

Follow the instructions to [create a flex template](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates) and image to avoid running gradle each time.


## Disclaimer

TBD
