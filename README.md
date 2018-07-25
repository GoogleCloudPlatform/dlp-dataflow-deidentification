## Data Tokenization PoC Using Dataflow/Beam 2.6 and DLP API  

This solution works for structure and semi structured data.   
Example #1: If you have a csv file containing columns like 'user_id' , 'password', 'account_number', 'credit_card_number' etc, this program can be used to tokenize all or subset of the columns.   

Example #2: If you have a csv file containing a field like 'comments' that may contain sensitive information like phone_number, credit_card_number, this program  can be used to inspect and then tokenize if found.  

Example #3: If you have a free blob like:   
"hasellus sit amet erat. Nulla tempus. Vivamus in felis eu sapien cursus vestibulum.".LU42 577W U2SJ IRLZ RSOO.Decentralized didactic implementation.0604998391122913.Self-enabling.unleash distributed ROI Gonzalo Homer.802-19-8847."In hac habitasse platea dictumst. Etiam faucibus cursus urna. Ut tellus.".AZ28 RSAD QAWQ RMMQ TRDZ XXKW YJXQ.Advanced systematic time-frame 3542994965622197.Function-based.productize efficient networks Melodi Ferdinand.581-74-6338."Proin eu mi. Nulla ac enim. In tempor, turpis nec euismod scelerisque, quam turpis adipiscing lorem, vitae mattis nibh ligula nec sem."    
You can see there are some sensitive information in the blob. This program will inspect and deidentify for the all 4 info types in the example. This is useful for the use case where chat log or log files may contain sensitive information.        
All the data used part of this project is mocked.  

### Getting Started

Clone the repo locally: git clone https://cloud-swarm.googlesource.com/data-tokenization

Example below includes use case to encrypt input file in GCS with a customer supplied key. PoC also works for google managed or customer managed encryption keys. Just take out encryption related arguments from the gradle run based on the encryption method. 
Run this for fully structured (example # 1 or 2) data by replacing argument related to your project:  

```

gradle run -DmainClass=com.google.swarm.tokenization.CSVBatchPipeline -Pargs="--streaming --project= --runner=DirectRunner  --inputFile=gs://<bucket>/pii-structured-data-*.csv --batchSize=4700 --dlpProject=<project_id> --deidentifyTemplateName=projects/<project_id>/deidentifyTemplates/8658110966372436613 --outputFile=gs://output-tokenization-data/output-structured-data --csek=CiQAbkxly/0bahEV7baFtLUmYF5pSx0+qdeleHOZmIPBVc7cnRISSQD7JBqXna11NmNa9NzAQuYBnUNnYZ81xAoUYtBFWqzHGklPMRlDgSxGxgzhqQB4zesAboXaHuTBEZM/4VD/C8HsicP6Boh6XXk= --csekhash=lzjD1iV85ZqaF/C+uGrVWsLq2bdN7nGIruTjT/mgNIE= --fileDecryptKeyName=gcs-bucket-encryption --fileDecryptKey=data-file-key"



```
Run this for non structured data (example #3) data by replacing argument related to your project 

```
gradle run -DmainClass=com.google.swarm.tokenization.TextStreamingPipeline -Pargs="--streaming --project=<project_id> --runner=DirectRunner  --inputFile=gs://<bucket_name>/pii-structured-data-*.csv --batchSize=<number> --dlpProject=<project_id> --deidentifyTemplateName=projects/<project_id>/deidentifyTemplates/<name> --inspectTemplateName=null --outputFile=gs://<bucket_name>/output-structured-data --csek=<kms_wrapped_key> --csekhash=<hash_key> --fileDecryptKeyName=<key_ring_name>--fileDecryptKey=<key_name>"
```
### Prerequisites

There are quite a few tasks before you can run this successfully:  

Create a GCP project and input output  GCS bucket  

```
After you are done, you should have the information available for the following args to be used from dataflow pipeline 
--inputFile=
--outputFile=
--project=
```

Create a customer supplied encryption key and obtain KMS wrapped key for the encryption key. If you want to use google managed or customer managed key, just keep the related args null. For example: csek=null 

```
Instruction for creating a key can be found here:
https://cloud.google.com/storage/docs/encryption/using-customer-supplied-keys

Creating a Key rings and Key can be found here:
https://cloud.google.com/kms/docs/quickstart

At the end you should have a KMS wrapped key that can be used for dataflow pipeline for the argument --csek like below:  
--csek=CiQAbkxly/0bahEV7baFtLUmYF5pSx0+qdeleHOZmIPBVc7cnRISSQD7JBqXna11NmNa9NzAQuYBnUNnYZ81xAoUYtBFWqzHGklPMRlDgSxGxgzhqQB4zesAboXaHuTBEZM/4VD/C8HsicP6Boh6XXk=
```
Create a hash of the encryption key:

```
 openssl base64 -d <<< encryption_key | openssl dgst -sha256 -binary | openssl base64
 
 After you run this command above by replacing with your own encription key created in the previous step, you will have to use this in the dataflow pipeline argument like:
 --csekhash=lzjD1iV85ZqaF/C+uGrVWsLq2bdN7nGIruTjT/mgNIE=
```
Create DLP template using API explorer 

```
There are two examples:
Please refer to the input file customer_data_pt.csv and de-identify template field_transformation_example.json for fully structured data set. After the template is created, you can pass it to dataflow pipeline. Please notice --inspectTemplateName is null as there is no need to inspect in this use case


Please refer to the input file customer_data.csv and teamplate infotype_transformations.json and inspectTemplate.json for a semi structure use case.
You will need to create KMW wrapped key for DLP tokenization. (Same process as described above in step #2)

https://developers.google.com/apis-explorer/#p/dlp/v2/

Please note there is NO code change required to make this program run. You will have to create your CSV data file and DLP template and pass it on to dataflow pipeline. 
```
### How the Batch Size works?
DLP API has a limit for payload size of 524KB /api call. That's why dataflow process will need to chunk it.  User will have to decide on how they would like to batch the request depending on number of rows and how big each row is.

```
Batch Size by number of rows/ call

--batchSize=4700
--batchSize=500
```


### How the Dataflow pipeline works?
It requires dataflow 2.6 snapshot build for split DoFn feature with FileIO.  
It polls for incoming file in given interval (10 seconds- hardcoded currently) in the GCS bucket and create a unbounded data source.  
There is one minute window to write to a target gcs bucket.
DF pipeline calls the KMS api to decrypt the key in memory so that file can be read   
If it's successful, file is successfully open and parsed by the batch size specified  
It creates a DLP table object as a content item with the chunk data and call DLP api to tokenize  by using the template passed  
At then end it writes the tokenized data in a GCS bucket.  
Please note for GMK or customer managed key use cases, there is no call to KMS is made.

### Known Issue
It requires split dofn feature which requires beam 2.6 to be able to successfully execiute gor growth.never() termination condition and watermark to advance.  
Also there is a known issue regarding GRPC version conflict with other google cloud products. That's why in gradle build file uses shaded jar concept to build and compile. Once the issue is resolved, build file can be updated to take out shading part.  

### How to generate KMS wrapped key
By using KMS API and pass a encryption key, you can generate a KMS wrapped key.

```
 curl -s -X POST "https://cloudkms.googleapis.com/v1/projects/<id>/locations/global/keyRings/<key_ring_name>/cryptoKeys/<key_name>:encrypt"  -d "{\"plaintext\":\"<key>"}"  -H "Authorization:Bearer $(gcloud auth application-default print-access-token)"  -H "Content-Type:application/json"

```
Example KMS Wrapped key: 

```
"CiQAB3BWoa2D0UQTpmkE/u0sP5PRQBpQr8EoS4rc8b7EWKHReMcSQQAlBr7wi5erTwHkb+hhjrzC7o/uu0xCf8e7/bvUTaNkfIfh+rs9782nDwlrF9EyOhQlXIaNbRRIxroyKekQuES+"
```
### Local Build

Clone the project 
Import as a gradle project in your IDE and execute gradle build.

### Dataflow template creation

you can create a dataflow template by following command:

```
gradle run -DmainClass=com.google.swarm.tokenization.CSVBatchPipeline  -Pargs="--streaming --project=scotia-tokezation --runner=DataflowRunner --templateLocation=gs://df-template/dlp-tokenization  --gcpTempLocation=gs://dlp-df-temp/data --workerHarnessContainerImage=dataflow.gcr.io/v1beta3/beam-java-streaming:beam-master-20180710"
```
And execute it either by UI, REST API or GCloud

```
gcloud dataflow jobs run test-run-3 --gcs-location gs://df-template/dlp-tokenization --parameters inputFile=gs://pii-batch-data/test-gmk-semi.csv,project=<id>,batchSize=100,deidentifyTemplateName=projects/<id>/deidentifyTemplates/6375801268847293878,outputFile=gs://output-tokenization-data/output-structured-data,inspectTemplateName=projects/<id>/inspectTemplates/7795191927316697091 
```
### To Do

- Unit Test and Code Coverage 
- There is a bug currently open for data flow template and value provider.




