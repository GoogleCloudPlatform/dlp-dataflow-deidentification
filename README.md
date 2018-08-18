# Data Tokenization PoC Using Dataflow/Beam 2.6 and DLP API  

This solution works for structure and semi structured data.   
Example #1: If you have a csv file containing columns like 'user_id' , 'password', 'account_number', 'credit_card_number' etc, this program can be used to tokenize all or subset of the columns.   

Example #2: If you have a csv file containing a field like 'comments' that may contain sensitive information like phone_number, credit_card_number, this program  can be used to inspect and then tokenize if found.  

Example #3: If you have a free blob like:   
"hasellus sit amet erat. Nulla tempus. Vivamus in felis eu sapien cursus vestibulum.".LU42 577W U2SJ IRLZ RSOO.Decentralized didactic implementation.0604998391122913.Self-enabling.unleash distributed ROI Gonzalo Homer.802-19-8847."In hac habitasse platea dictumst. Etiam faucibus cursus urna. Ut tellus.".AZ28 RSAD QAWQ RMMQ TRDZ XXKW YJXQ.Advanced systematic time-frame 3542994965622197.Function-based.productize efficient networks Melodi Ferdinand.581-74-6338."Proin eu mi. Nulla ac enim. In tempor, turpis nec euismod scelerisque, quam turpis adipiscing lorem, vitae mattis nibh ligula nec sem."    
You can see there are some sensitive information in the blob. This program will inspect and deidentify for the all 4 info types in the example. This is useful for the use case where chat log or log files may contain sensitive information.        
All the data used part of this project is mocked.  

### Getting Started

Clone the repo locally.

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
It requires split dofn feature which requires beam 2.6 to be able to successfully execute growth.never() termination condition and watermark to advance. https://github.com/apache/beam/pull/5836    
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
gradle run -DmainClass=com.google.swarm.tokenization.CSVBatchPipeline  -Pargs="--streaming --project=<id> --runner=DataflowRunner --templateLocation=gs://df-template/dlp-tokenization  --gcpTempLocation=gs://dlp-df-temp/data --workerHarnessContainerImage=dataflow.gcr.io/v1beta3/beam-java-streaming:beam-master-20180710"
```
If the template is created successfully, you should see a meesage "Template is created sucessfully" in the console log. 

There is a metadata file needs to be uploaded in the same location where the template is created. Copy the following JSON file and paste it in a file called dlp-tokenization_metadata (Please don't save as json extension)

```
{
  "name": "dlp-tokenization",
  "description": "DLP Data Tokenization Pipeline",
  "parameters": [{
    "name": "inputFile",
    "label": "GCS File Path to Tokenize",
    "help_text": "gs://MyBucket/object",
    "regexes": ["^gs:\/\/[^\n\r]+$"],
    "is_optional": false
  },
  {
    "name": "outputFile",
    "label": "Location of GCS path where Tokenized Data will be written",
    "help_text": "Path and filename prefix for writing output files. ex: gs://MyBucket/object",
    "regexes": ["^gs:\/\/[^\n\r]+$"],
	 "is_optional": false
  },
  {
      "name": "project",
      "label": "Name of the Host Project",
      "help_text": "project_id",
      "is_optional": false
    },
    {
      "name": "batchSize",
      "label": "batch size in number of rows",
      "help_text": "4700, 200",
		"is_optional": false
	},
   {
      "name": "dlpProject",
      "label": "Name of the DLP Project",
      "help_text": "project_id",
      "is_optional": true
    },
    {
      "name": "inspectTemplateName",
      "label": "inspect template name",
      "help_text": "null, projects/{dlp_prject_name}/inspectTemplates/{name}",
		"is_optional": true
	},
   {
     "name": "deidentifyTemplateName",
     "label": "deidentify template name",
     "help_text": "null, projects/{dlp_prject_name}/deidentifyTemplates/{name}",
	  "is_optional": false
	},
  {
     "name": "csek",
     "label": "Client Supplied Encryption key (KMS Wrapped)",
     "help_text": "CiQAbkxly/0bahEV7baFtLUmYF5pSx0+qdeleHOZmIPBVc7cnRISSQD7JBqXna11NmNa9NzAQuYBnUNnYZ81xAoUYtBFWqzHGklPMRlDgSxGxgzhqQB4zesAboXaHuTBEZM/4VD/C8HsicP6Boh6XXk=",
     "is_optional": true
   },
   {
     "name": "csekhash",
     "label": "Hash of CSEK",
     "help_text": "lzjD1iV85ZqaF/C+uGrVWsLq2bdN7nGIruTjT/mgNIE=",
	  "is_optional": true
	},
  {
    "name": "fileDecryptKeyName",
    "label": "Key Ring For Input File Encryption",
    "help_text": "gcs-bucket-encryption",
    "is_optional": true
	},
   {
     "name": "fileDecryptKey",
     "label": "Key Name For Input File Encryption",
     "help_text": "data-file-key",
     "is_optional": true
 	}
  
  ]
}



```


And execute it either by UI, REST API or GCloud.

```
gcloud dataflow jobs run test-run-3 --gcs-location gs://df-template/dlp-tokenization --parameters inputFile=gs://pii-batch-data/test-gmk-semi.csv,project=<id>,batchSize=100,deidentifyTemplateName=projects/<id>/deidentifyTemplates/6375801268847293878,outputFile=gs://output-tokenization-data/output-structured-data,inspectTemplateName=projects/<id>/inspectTemplates/7795191927316697091 
```
### DLP template used in the use cases
#####Example 1: Fully Structured Data 
Sample input format in CSV:
UserId,Password,PhoneNumber,CreditCard,SIN,AccountNumber
dfellowes0@answers.com,dYmeZB,413-686-1509,374283210039640,878-44-9652,182001096-2
rkermath1@ox.ac.uk,OKJtHxB,859-180-4370,5038057247505321293,275-41-2120,793375128-8
snagle2@ftc.gov,VQsYF9Iv68,569-519-7788,3533613600755599,265-63-9123,644327532-2

DLP Deidentify Template Used:

```
{
 "name": "projects/{id}/deidentifyTemplates/8658110966372436613",
 "createTime": "2018-06-19T15:58:32.214456Z",
 "updateTime": "2018-06-19T15:58:32.214456Z",
 "deidentifyConfig": {
  "recordTransformations": {
   "fieldTransformations": [
    {
     "fields": [
      {
       "name": "SIN"
      },
      {
       "name": "AccountNumber"
      },
      {
       "name": "CreditCard"
      },
      {
       "name": "PhoneNumber"
      }
     ],
     "primitiveTransformation": {
      "cryptoReplaceFfxFpeConfig": {
       "cryptoKey": {
        "kmsWrapped": {
         "wrappedKey": "CiQAku+QvbDmstgYj4NEaoV6FGuB8l3jjWcUiyzJ+HR8NXYZSCASQQBdX/BxUhNiRixvCZnR5/zjFd8D0w9td1w6LUHccIb8HL0s+bK9iOzdllgcXRDC3X9tjx2oqI+S6lFd9tqE5ftd",
         "cryptoKeyName": "projects/{id}/locations/global/keyRings/customer-pii-data-ring/cryptoKeys/pii-data-key"
        }
       },
       "customAlphabet": "1234567890-"
      }
     }
    }
   ]
  }
 }
}
```
#####Example 2: Semi Structured Data 
Sample input format in CSV:
UserId,SIN,AccountNumber,Password,CreditCard,Comments
ofakeley0@elpais.com,607-82-9963,679647039-7,5433c541-a735-4783-ac1b-bdb1f95ba7b5,6706970503473868,Please change my number to. Thanks
cstubbington1@ibm.com,543-43-5623,466928803-2,b2892e74-9588-4c23-9645-ea4fdf4729e8,3549127839068106,Please change my number to 674-486-9054. Thanks
ekaman2@home.pl,231-68-8007,242426152-0,ce7e4400-92ea-4ba9-8758-5c6215b68b47,201665969359626,Please change my number to 430-349-3493. Thanks

DLP Deidentify Template Used:

```
{
 "name": "projects/{id}/deidentifyTemplates/6375801268847293878",
 "createTime": "2018-06-18T19:59:13.902712Z",
 "updateTime": "2018-06-19T12:25:03.554233Z",
 "deidentifyConfig": {
  "recordTransformations": {
   "fieldTransformations": [
    {
     "fields": [
      {
       "name": "SIN"
      },
      {
       "name": "AccountNumber"
      },
      {
       "name": "CreditCard"
      }
     ],
     "primitiveTransformation": {
      "cryptoReplaceFfxFpeConfig": {
       "cryptoKey": {
        "kmsWrapped": {
         "wrappedKey": "CiQAku+QvbDmstgYj4NEaoV6FGuB8l3jjWcUiyzJ+HR8NXYZSCASQQBdX/BxUhNiRixvCZnR5/zjFd8D0w9td1w6LUHccIb8HL0s+bK9iOzdllgcXRDC3X9tjx2oqI+S6lFd9tqE5ftd",
         "cryptoKeyName": "projects/{id}/locations/global/keyRings/customer-pii-data-ring/cryptoKeys/pii-data-key"
        }
       },
       "customAlphabet": "1234567890ABCDEF-"
      }
     }
    },
    {
     "fields": [
      {
       "name": "Comments"
      }
     ],
     "infoTypeTransformations": {
      "transformations": [
       {
        "infoTypes": [
         {
          "name": "PHONE_NUMBER"
         }
        ],
        "primitiveTransformation": {
         "cryptoReplaceFfxFpeConfig": {
          "cryptoKey": {
           "kmsWrapped": {
            "wrappedKey": "CiQAku+QvbDmstgYj4NEaoV6FGuB8l3jjWcUiyzJ+HR8NXYZSCASQQBdX/BxUhNiRixvCZnR5/zjFd8D0w9td1w6LUHccIb8HL0s+bK9iOzdllgcXRDC3X9tjx2oqI+S6lFd9tqE5ftd",
            "cryptoKeyName": "projects/{id}/locations/global/keyRings/customer-pii-data-ring/cryptoKeys/pii-data-key"
           }
          },
          "customAlphabet": "1234567890-",
          "surrogateInfoType": {
           "name": "[PHONE]"
          }
         }
        }
       }
      ]
     }
    }
   ]
  }
 }
}
 
```

DLP Inspect template:

```

{
 "name": "projects/{id}/inspectTemplates/7795191927316697091",
 "createTime": "2018-06-23T02:12:32.062789Z",
 "updateTime": "2018-06-23T02:12:32.062789Z",
 "inspectConfig": {
  "infoTypes": [
   {
    "name": "PHONE_NUMBER"
   }
  ],
  "minLikelihood": "POSSIBLE",
  "limits": {
  }
 }
}

```

#####Example 3: Non Structured Data 
Sed ante. Vivamus tortor. Duis mattis egestas metus.".SI84 7084 7501 2230 378.Operative dynamic frame.5602235457198185.non-volatile.innovate collaborative supply-chains
Selma Jade.407-24-8213."Morbi non lectus. Aliquam sit amet diam in magna bibendum imperdiet. Nullam orci pede, venenatis non, sodales sed, tincidunt eu, felis.".MD88 XCZD GPMR JX8X 8BN8 9RTA.Mandatory attitude-oriented migration.5285030805458686.function.harness 24/365 markets
Helga Lane.870-09-7239."Suspendisse potenti. In eleifend quam a odio. In hac habitasse platea dictumst.

DLP Deidentify Template Used:

```
{
 "name": "projects/{id}/deidentifyTemplates/2206100676278191052",
 "createTime": "2018-06-21T14:29:24.595407Z",
 "updateTime": "2018-06-21T14:29:24.595407Z",
 "deidentifyConfig": {
  "infoTypeTransformations": {
   "transformations": [
    {
     "primitiveTransformation": {
      "replaceWithInfoTypeConfig": {
      }
     }
    }
   ]
  }
 }
}
 
```

DLP Inspect template:

```

{
 "name": "projects/{id}/inspectTemplates/3113085388824253847",
 "createTime": "2018-06-21T14:32:49.905406Z",
 "updateTime": "2018-06-21T14:32:49.905406Z",
 "inspectConfig": {
  "infoTypes": [
   {
    "name": "CREDIT_CARD_NUMBER"
   },
   {
    "name": "PERSON_NAME"
   },
   {
    "name": "US_SOCIAL_SECURITY_NUMBER"
   },
   {
    "name": "IBAN_CODE"
   }
  ],
  "minLikelihood": "VERY_UNLIKELY",
  "limits": {
  }
 }
}

```



### To Do

- Unit Test and Code Coverage 
- There is a bug currently open for data flow template and value provider.




