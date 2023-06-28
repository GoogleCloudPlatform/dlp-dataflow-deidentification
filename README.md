
# Inspect, de-identify and re-identify sensitive data using Cloud DLP and Dataflow
> This repo contains a reference implementation of an end to end data tokenization solution. The solution is designed to migrate sensitive data to BigQuery after passing it through inspection/de-identification/re-identification Dataflow pipelines implemented using Cloud DLP. Please check out the links below for reference guides.

## Table of Contents

* [Architecture](#architecture)

* [Concepts](#concepts)

* [Costs](#costs)

* [Tutorial](#tutorial)

  * [Prerequisites](#pre-requisites)
  * [Compile the code](#compile-the-code)
  * [Run the samples](#run-the-samples)
    * [Inspection](#inspection)
    * [De-identification](#de-identification)
    * [Re-identification](#re-identification-from-bigquery)
  * [Pipeline parameters](#pipeline-parameters)
  * [Supported file formats](#supported-file-formats)
  * [Amazon S3 scanner](#amazon-s3-scanner)

* [Adapt this pipeline for your use cases](#adapt-this-pipeline-for-your-use-cases)

* [Troubleshooting](#troubleshooting)

* [Dataflow DAG](#dataflow-dag)

* [Advanced topics](#advanced-topics)

* [Disclaimer](#disclaimer)


## Architecture
The solution comprises two types of pipelines (based on the DLP transformation type). To view a job graph of the pipeline, see [Dataflow DAG](#dataflow-dag).
1. Inspection and de-identification
2. Re-identification

### Inspection and De-identification
![Reference Architecture](diagrams/inspect-deid-architecture.png)

The pipeline can be used for CSV, TSV, Avro, and JSONL files stored in either Cloud Storage or Amazon S3 bucket. It uses State and Timer API for efficient batching to process the files in optimal manner.
The results of the inspection or de-identification is written to a BigQuery table

### Re-identification
![Reference Architecture](diagrams/reid-architecture.png)

The pipeline for re-identification workflow is used to read data from BigQuery table and publish the re-identified data in a secure pub sub topic.

## Concepts

Please refer to the following list of resources to understand key concepts related to Cloud DLP and Dataflow.

1. [Cloud Data Loss Prevention - Quick Start & Guides](https://cloud.google.com/dlp/docs/dlp-bigquery)
2. [De-identification and re-identification of PII in large-scale datasets using Cloud DLP](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp).
3. [Cloud DLP templates](https://cloud.google.com/dlp/docs/concepts-templates).
4. [Automated Dataflow Pipeline to De-identify PII Dataset](https://cloud.google.com/dataflow/docs/guides/templates/provided/dlp-text-to-bigquery).
5. [Validate Dataset in BigQuery and Re-identify using Dataflow](https://cloud.google.com/solutions/validating-de-identified-data-bigquery-re-identifying-pii-data).
6. [Inspecting storage and databases for sensitive data](https://cloud.google.com/dlp/docs/inspecting-storage)
7. [Dataflow Pipeline Options](https://cloud.google.com/dataflow/docs/reference/pipeline-options)
8. [Cloud DLP Quotas and Limits](https://cloud.google.com/dlp/limits)

## Costs

This tutorial uses billable components of Google Cloud, including the following:

* [Dataflow](https://cloud.google.com/dataflow/pricing)
* [Cloud Storage](https://cloud.google.com/storage/pricing)
* [Cloud Data Loss Prevention](https://cloud.google.com/dlp/pricing)
* [Cloud KMS](https://cloud.google.com/kms/pricing)
* [BigQuery](https://cloud.google.com/bigquery/pricing)

Use the [pricing calculator](https://cloud.google.com/products/calculator) to generate a cost estimate based on your
projected usage.

## Tutorial

### Prerequisites

1. [Create a Google Cloud project](https://console.cloud.google.com/projectselector2/home/dashboard).

2. Make sure that [billing is enabled](https://support.google.com/cloud/answer/6293499#enable-billing) for your Google
   Cloud project.

3. Use the link below to open Cloud Shell.

   [![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https://github.com/GoogleCloudPlatform/dlp-dataflow-deidentification.git)

4. Run the following commands to set up the data tokenization solution in your Google Cloud project. 

    ```
    gcloud config set project <project_id>
    sh setup-data-tokeninzation-solution-v2.sh
    ```

    Script (setup-data-tokenization-solution-v2.sh) handles following tasks:

     * Creates a bucket ({project-id}-demo-data) in us-central1 region and [uploads a sample dataset](http://storage.googleapis.com/dataflow-dlp-solution-sample-data/sample_data_scripts.tar.gz) with <b>mock</b> PII data.

     * Creates a BigQuery dataset (demo_dataset) in US multi-region to store the tokenized data.

     * Creates a [KMS wrapped key(KEK)](https://cloud.google.com/kms/docs/envelope-encryption) by creating an automatic [TEK](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp#token_encryption_keys) (Token Encryption Key).

     * Creates DLP [inspect, de-identification and re-identification templates](https://cloud.google.com/solutions/creating-cloud-dlp-de-identification-transformation-templates-pii-dataset#creating_the_cloud_dlp_templates) with the KEK and crypto-based transformations identified in this [section of the guide](https://cloud.google.com/solutions/de-identification-re-identification-pii-using-cloud-dlp#determining_transformation_type)

     * Creates a service account for running the DLP pipeline (creates a custom role).

     * Emits a set_env.sh, which you can use to set temporary environment variables while triggering the DLP pipelines.

5. Run set_env.sh
    ```
    source set_env.sh
    ```


### Compile the code

```
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
--batchSize=200000 \
--DLPMethod=INSPECT" 
```

This command will trigger a streaming inspection Dataflow pipeline that will process all the CSV files in the demo-data 
GCS bucket (specify in _filePattern_ parameter). The inspection findings can be found in the BQ dataset (_dataset_ 
parameter) table.

#### De-Identification

```
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 \
-Pargs=" --region=${REGION} \
--project=${PROJECT_ID} \
--streaming --enableStreamingEngine \
--tempLocation=gs://${PROJECT_ID}-demo-data/temp \
--numWorkers=2 --maxNumWorkers=3 \
--runner=DataflowRunner \
--filePattern=gs://${PROJECT_ID}-demo-data/*.csv \
--dataset=${BQ_DATASET_NAME}   \
--inspectTemplateName=${INSPECT_TEMPLATE_NAME} \
--deidentifyTemplateName=${DEID_TEMPLATE_NAME} \
--batchSize=200000\
--DLPMethod=DEID" 
```

This command will trigger a streaming de-identification Dataflow pipeline that will process all the CSV files in the 
demo-data GCS bucket (specify in _filePattern_ parameter). The de-identified results can be found in the BQ dataset 
(_dataset_ parameter) tables with the same names as input files, respectively.

You can run some quick [validations](https://web.archive.org/web/20220331191143/https://cloud.google.com/architecture/validating-de-identified-data-bigquery-re-identifying-pii-data) 
in a BigQuery table to check the tokenized data.


#### Re-identification from BigQuery

1. Export the Standard SQL query to read data from BigQuery. For example:

```
export QUERY="select ID,Card_Number,Card_Holders_Name from \`${PROJECT_ID}.${BQ_DATASET_NAME}.CCRecords_1564602828\` where safe_cast(Credit_Limit as int64)>100000 and safe_cast (Age as int64)>50 group by ID,Card_Number,Card_Holders_Name limit 10"
```

2. Create a Cloud Storage file with the following query:

```
export REID_QUERY_BUCKET=<name>
cat << EOF | gsutil cp - gs://${REID_QUERY_BUCKET}/reid_query.sql
${QUERY}
EOF
```

3. Create a Pub/Sub topic. For more information, see [create a topic](https://cloud.google.com/pubsub/docs/create-topic#create_a_topic)

4. Run the pipeline by passing the required parameters:

```
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 
-Pargs="--region=${REGION} 
--project=${PROJECT_ID} 
--tempLocation=gs://${PROJECT_ID}-demo-data/temp 
--numWorkers=1 --maxNumWorkers=2 
--runner=DataflowRunner 
--tableRef=${PROJECT_ID}:${BQ_DATASET_NAME}.<table> 
--dataset=${BQ_DATASET_NAME} 
--topic=projects/${PROJECT_ID}/topics/<topic_name> 
--autoscalingAlgorithm=THROUGHPUT_BASED 
--workerMachineType=n1-highmem-4 
--deidentifyTemplateName=${REID_TEMPLATE_NAME} 
--DLPMethod=REID 
--keyRange=1024 
--queryPath=gs://${REID_QUERY_BUCKET}/reid_query.sql"
```

This command will trigger a batch re-identification Dataflow pipeline that will process all the records from the query 
stored in _reid_query.sql_. The re-identified results can be found in the BQ 
dataset (_dataset_ parameter) table with the name of input table as suffix.


### Pipeline Parameters

| Pipeline Option                  | Description                                                                                                                                                                                                                                                        | Used in Operations  |
|----------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| `region`                         | Specifies a regional endpoint for deploying your Dataflow jobs.                                                                                                                                                                                                    | All                 |
| `project`                        | The project ID for your Google Cloud project.                                                                                                                                                                                                                      | All                 |
| `streaming`                      | true if streaming pipeline                                                                                                                                                                                                                                         | INSPECT/DEID        |
| `enableStreamingEngine`          | Specifies whether Dataflow Streaming Engine is enabled or disabled                                                                                                                                                                                                 | INSPECT/DEID        |
| `tempLocation`                   | Cloud Storage path for temporary files. Must be a valid Cloud Storage URL                                                                                                                                                                                          | All                 |
| `numWorkers`                     | (Optional) The initial number of Compute Engine instances to use when executing your pipeline. This option determines how many workers the Dataflow service starts up when your job begins.                                                                        | All                 |
| `maxNumWorkers`                  | (Optional) The maximum number of Compute Engine instances to be made available to your pipeline during execution. This value can be higher than the initial number of workers (specified by numWorkers) to allow your job to scale up, automatically or otherwise. | All                 |
| `runner`                         | DataflowRunner                                                                                                                                                                                                                                                     | All                 |
| `inspectTemplateName`            | DLP inspect template name                                                                                                                                                                                                                                          | INSPECT/DEID        | 
| `deidentifyTemplateName`         | DLP de-identify template name                                                                                                                                                                                                                                      | All                 |
| `DLPMethod`                      | Type of DLP operation to perform - INSPECT/DEID/REID                                                                                                                                                                                                               | All                 |
| `batchSize`                      | (Optional) Batch size for the DLP API, default is 500K                                                                                                                                                                                                             | All                 |
| `dataset`                        | BigQuery dataset to write the inspection or de-identification results to or to read from in case of re-identification                                                                                                                                              | All                 |
| `recordDelimiter`                | (Optional) Record delimiter                                                                                                                                                                                                                                        | INSPECT/DEID        |
| `columnDelimiter`                | Column Delimiter - only required in case of custom delimiter                                                                                                                                                                                                       | INSPECT/DEID        | 
| `tableRef`                       | BigQuery table to export from in the form `<project>:<dataset>.<table>`                                                                                                                                                                                            | REID                |
| `queryPath`                      | Query file for re-identification                                                                                                                                                                                                                                   | REID                |
| `headers`                        | DLP table headers- Required for JSONL file type                                                                                                                                                                                                                    | INSPECT/DEID        |
| `numShardsPerDLPRequestBatching` | (Optional) Number of shards for DLP request batches. Can be used to control parallelism of DLP requests. Default value is 100                                                                                                                                      | All                 |
| `numberOfWorkerHarnessThreads`   | (Optional) The number of threads per each worker harness process                                                                                                                                                                                                   | All                 |
| `dlpApiRetryCount`               | (Optional) Number of retries in case of transient errors in DLP API. Default value is 10                                                                                                                                                                           | All                 |
| `initialBackoff`                 | (Optional) Initial backoff (in seconds) for retries with exponential backoff. Default is 5s                                                                                                                                                                        | All                 |

For more details, see [Dataflow Pipeline Options](https://cloud.google.com/dataflow/docs/reference/pipeline-options).

### Supported File Formats

#### 1. CSV
For sample commands for processing CSV files, see [Run the samples](#run-the-samples).

#### 2. TSV

The pipeline supports TSV file format which uses TAB as the column delimiter. No additional changes are required in pipeline options.
```
gradle build ... -Pargs="... --filePattern=gs://<bucket_name>/small_file.tsv"

```
#### 3. JSONL

The pipeline supports JSONL file format where each line is a valid JSON object and newline character separate JSON objects. For a sample file, see the [test resources](src/test/resources/CCRecords_sample.jsonl). 
To run the pipeline for JSONL files, the list of comma-separated headers also needs to be passed in the pipeline options. 
```
// Copy the sample jsonl file to Cloud Storage
gsutil cp ./src/test/resources/CCRecords_sample.jsonl gs://<bucket>/

// Run the pipeline using following command
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 -Pargs=" --region=<region> --project=<projct_id> --streaming --enableStreamingEngine --tempLocation=gs://<bucket>/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --filePattern=gs://<path>.jsonl --dataset=<name>   --inspectTemplateName=<inspect_template> --deidentifyTemplateName=<deid_tmplate> --DLPMethod=DEID --headers=<comma_separated_list_of_headers>"
```
#### 4. Avro

Avro files are handled in the same way as CSV files. No additional changes are required to run the pipeline.
For sample data, see the [avro](src/test/resources/avro) directory.

#### 5. CSV files with custom delimiter 

It is possible to provide CSV files with custom delimiter. The delimiter has to be passed in the pipeline option as "--columnDelimiter". 
```
gradle build ... -Pargs="... --columnDelimiter=|"
```



### Amazon S3 Scanner

To use Amazon S3 as a source of input files, use AWS credentials as instructed below.

Export the AWS access key, secret key, and credentials provider to environment variables. 

```
export AWS_ACCESS_KEY="<access_key>"
export AWS_SECRET_KEY="<secret_key>"
export AWS_CRED="{\"@type\":\"AWSStaticCredentialsProvider\",\"awsAccessKeyId\":\"${AWS_ACCESS_KEY}\",\"awsSecretKey\":\"${AWS_SECRET_KEY}\"}"
```

Use Gradle to build and run the job to perform DLP operations on a CSV file stored in Amazon S3. The results will be written to BigQuery.

```
gradle build

// inspect is default as DLP Method; For deid: --DLPMethod=DEID
gradle run -DmainClass=com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2 -Pargs="--region=<region> --project=<project_id> --streaming --enableStreamingEngine --tempLocation=gs://<bucket>/temp --numWorkers=1 --maxNumWorkers=2 --runner=DataflowRunner --filePattern=s3://<bucket>>/file.csv --dataset=<name>  --inspectTemplateName=<inspect_template> --deidentifyTemplateName=<deid_tmplate> --awsRegion=<aws_region> --awsCredentialsProvider=$AWS_CRED"
```

#### Parameters

* --awsRegion: The region where the AWS resources reside.

* --awsCredentialsProvider: The AWS credentials provider.


## Adapt this pipeline for your use cases
The DLP templates utilized in this tutorial are specifically tailored for inspecting, de-identifying, and re-identifying sample data containing simulated personally identifiable information (PII). It is important to note that when working with your own data, you should create custom DLP templates that align with the characteristics of the data being processed. Please refer to the links below to create your own templates.
1. [Create your own inspection templates and run inspection on sample data](https://cloud.google.com/dlp/docs/creating-templates-inspect)
2. [Create de-identification templates and run de-identification on sample data](https://cloud.google.com/dlp/docs/creating-templates-deid)


## Troubleshooting

Following are some issues/errors that one may encounter while running the pipeline, and the ways you can avoid or recover from them.

* Duplicate rows: When writing data to a BigQuery table, Cloud DLP might write duplicate rows.

    The project uses the Streaming Inserts API of BigQuery which by default enables best-effort deduplication mechanism. For a possible solution, see the knowledge base article, checkout [high number of duplicates in Dataflow pipeline streaming inserts to BigQuery](https://cloud.google.com/knowledge/kb/high-number-of-duplicates-in-dataflow-pipeline-streaming-inserts-to-bigquery-000004276?authuser=0).


* Errors in DLPTransform step: 

  (The detailed errors can be viewed in worker logs for the mentioned PTransform.)
   
    1. ```INVALID_ARGUMENT: Too many findings to de-identify. Retry with a smaller request.```

       DLP has a max findings per request [limit](https://cloud.google.com/dlp/limits#content-redaction-limits) of 3000. Run the pipeline again with a smaller batch size.
       
    2. ```RESOURCE_EXHAUSTED: Quota exceeded for quota metric 'Number of requests' and limit 'Number of requests per minute' of service 'dlp.googleapis.com'```

       This can happen if the Dataflow pipeline is being run with a small batch size. Re-run the pipeline with a larger batch size.

       If increasing the batch size is not possible or if the issue persists even after reaching the maximum batch size, consider trying one of the following options:

       * The pipeline incorporates a retry mechanism for DLP API calls, utilizing an exponential delay approach. To enhance the retry behavior, you can adjust the value of the `dlpApiRetryCount` parameter. For more information, please refer to the [pipeline parameters](#pipeline-parameters) section, specifically the `dlpApiRetryCount` and `initialBackoff` parameters.

       * The pipeline includes a parameter called `numShardsPerDLPRequestBatching`. Decreasing this value below the default (100) will result in a lower number of concurrent requests sent to DLP.

       * Verify if there are any other pipelines or clients generating DLP API requests.

       * Consider submitting a request to increase the quota limit. Refer [increase dlp quota](https://cloud.google.com/dlp/limits#increases).


## Dataflow DAG

For inspection and de-identification:
![v2_dag_](diagrams/dlp_dag_new.png)



For re-identification

![v2_dag_](diagrams/dlp_reid_dag.png)



## Advanced topics
Dataflow templates allow you to package a Dataflow pipeline for deployment. Instead of having to build the pipeline everytime, you can create flex templates and deploy the template by using the Google Cloud console, the Google Cloud CLI, or REST API calls.
For more details, refer [Dataflow templates](https://cloud.google.com/dataflow/docs/concepts/dataflow-templates) and [flex templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates).

## Some Considerations

The behavior of the pipeline is dependent on factors such as the length of the record, the number of findings per record, the DLP API quota on the project, and other applications/pipelines generating DLP API traffic.
Customers may need to adjust the parameters mentioned above and should refer to the troubleshooting section when they encounter errors.
Most errors observed in the pipeline indicate that the parameters need to be adjusted.
There may be error scenarios that the pipeline doesn't currently handle and may require code changes.
Therefore, it is important not to assume that this solution is production-ready due to the reasons mentioned above.

