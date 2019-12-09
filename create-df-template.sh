#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.#!/usr/bin/env bash
# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
 
set -x 

PROJECT_ID=$1
AWS_ACCESS_KEY=$2
API_KEY=$3
AWS_SECRET_KEY=$4
S3_BUCKET_URL=$5
GCS_BUCKET_URL=$6
AWS_REGION=$7
BQ_DATASET=$8
INSPECT_CONFIG="@gcs-s3-inspect-config.json"
INSPECT_TEMPLATE_OUTPUT="inspect-template.json"
DLP_API_ROOT_URL="https://dlp.googleapis.com"
INSPECT_TEMPLATE_API="${DLP_API_ROOT_URL}/v2/projects/${PROJECT_ID}/inspectTemplates"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer ${API_KEY}" \
 "${INSPECT_TEMPLATE_API}"`` \
 -d "${INSPECT_CONFIG}"\
 -o "${INSPECT_TEMPLATE_OUTPUT}"
more ${INSPECT_TEMPLATE_OUTPUT}
INSPECT_TEMPLATE_NAME=$(jq -c '.name' ${INSPECT_TEMPLATE_OUTPUT})
# publicly hosted image
DYNAMIC_TEMPLATE_BUCKET_SPEC=gs://dynamic-template/dynamic_template_dlp_inspect.json
JOB_NAME="dlp-inspect-pipeline-`date +%Y%m%d-%H%M%S-%N`"
echo $JOB_NAME
GCS_STAGING_LOCATION=gs://dynamic-template/log
TEMP_LOCATION=gs://dynamic-template/temp
PARAMETERS_CONFIG='{  
   "jobName":"'$JOB_NAME'",
   "parameters":{  
      "streaming":"true",
	  "enableStreamingEngine":"true",
	  "autoscalingAlgorithm":"NONE",
      "workerMachineType": "n1-standard-8",
      "numWorkers":"50",
      "maxNumWorkers":"50",
      "awsAccessKey":"'$AWS_ACCESS_KEY'",
	  "awsSecretKey":"'$AWS_SECRET_KEY'",
	  "s3BucketUrl":"'$S3_BUCKET_URL'",
	  "gcsBucketUrl":"'$GCS_BUCKET_URL'",
	  "inspectTemplateName":'$INSPECT_TEMPLATE_NAME',
	  "s3ThreadPoolSize":"1000",
	  "maxConnections":"1000000",
	  "socketTimeout":"100",
	  "connectionTimeout":"100",
	  "tempLocation":"'$TEMP_LOCATION'",
	  "awsRegion":"'$AWS_REGION'",
	  "dataSetId":"'$BQ_DATASET'",	  
	}
}'
DF_API_ROOT_URL="https://dataflow.googleapis.com"
TEMPLATES_LAUNCH_API="${DF_API_ROOT_URL}/v1b3/projects/${PROJECT_ID}/templates:launch"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer ${API_KEY}" \
 "${TEMPLATES_LAUNCH_API}"`
 `"?validateOnly=false"`
 `"&dynamicTemplate.gcsPath=${DYNAMIC_TEMPLATE_BUCKET_SPEC}"` \
 `"&dynamicTemplate.stagingLocation=${GCS_STAGING_LOCATION}" \
 -d "${PARAMETERS_CONFIG}"
