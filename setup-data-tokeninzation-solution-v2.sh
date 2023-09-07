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

# please make sure you have owner permission in your project 
set -x 
# export some env variables
export PROJECT_ID=$(gcloud config get-value project)
export DATA_STORAGE_BUCKET=${PROJECT_ID}-demo-data 
export TEK=$(openssl rand -base64 32)
export KEY_RING_NAME=demo-key-ring
export KEY_NAME=demo-key
export KEK_FILE_NAME=kek.json
export TOKENIZING_ROLE_NAME="dlp_tokenizing_runner"
export PROJECT_NUMBER=$(gcloud projects list --filter=${PROJECT_ID} --format="value(PROJECT_NUMBER)") 
export SERVICE_ACCOUNT_NAME=demo-service-account
export REGION=us-central1
export BQ_DATASET_NAME=demo_dataset
export GCS_NOTIFICATION_TOPIC=${PROJECT_ID}-gcs-notification-topic
# enable the required APIs
gcloud services enable dlp.googleapis.com
gcloud services enable cloudkms.googleapis.com
gcloud services enable bigquery
gcloud services enable storage_component
gcloud services enable dataflow
gcloud services enable cloudbuild.googleapis.com
gcloud services enable pubsub.googleapis.com
# create BQ dataset. Table will be dynamically generated from dataflow pipeline
bq --location=US mk -d --description "De-Identified PII Dataset" ${BQ_DATASET_NAME}
# create a data bucket to store the PII data
gsutil mb -c standard -l ${REGION} gs://${DATA_STORAGE_BUCKET}
# allow some additional access to cloud build service account
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role roles/cloudkms.cryptoKeyEncrypter
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member serviceAccount:$PROJECT_NUMBER@cloudbuild.gserviceaccount.com --role roles/cloudkms.admin
# trigger the first cloud build script to create the KEK
gcloud builds submit . --config dlp-demo-part-1-crypto-key.yaml --substitutions _GCS_BUCKET_NAME=gs://${DATA_STORAGE_BUCKET},_KEY_RING_NAME=${KEY_RING_NAME},_KEY_NAME=${KEY_NAME},_TEK=${TEK},_KEK=${KEK_FILE_NAME},_API_KEY=$(gcloud auth print-access-token)

# Create service account to be used for running dataflow pipeline
gcloud iam service-accounts create  ${SERVICE_ACCOUNT_NAME} --project="${PROJECT_ID}" --description="Service Account for DLP Tokenizing pipelines." --display-name="DLP Tokenizing pipelines"

gcloud iam roles create ${TOKENIZING_ROLE_NAME} --project=${PROJECT_ID} --file=dlp_tokenizing_runner_permissions.yaml

SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@$(echo $PROJECT_ID | awk -F':' '{print $2"."$1}' | sed 's/^\.//').iam.gserviceaccount.com"
export SERVICE_ACCOUNT_EMAIL

gcloud projects add-iam-policy-binding ${PROJECT_ID}  --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role=projects/${PROJECT_ID}/roles/${TOKENIZING_ROLE_NAME}
gcloud projects add-iam-policy-binding ${PROJECT_ID} --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" --role=roles/dataflow.worker

# trigger the cloud build script to create DLP templates
gcloud builds submit . --config dlp-demo-part-2-dlp-template.yaml --substitutions _KEK_CONFIG_FILE=gs://${DATA_STORAGE_BUCKET}/${KEK_FILE_NAME},_GCS_BUCKET_NAME=gs://${DATA_STORAGE_BUCKET},_API_KEY=$(gcloud auth print-access-token)

# create Pub/Sub topic and enable GCS notification to the pub/sub topic
gcloud pubsub topics create ${GCS_NOTIFICATION_TOPIC}
gsutil notification create -e OBJECT_FINALIZE -t ${GCS_NOTIFICATION_TOPIC} -f json gs://${DATA_STORAGE_BUCKET}

# download the json file to parse template name using jq
gsutil cp gs://${DATA_STORAGE_BUCKET}/deid-template.json .
gsutil cp gs://${DATA_STORAGE_BUCKET}/inspect-template.json .
gsutil cp gs://${DATA_STORAGE_BUCKET}/reid-template.json .
export DEID_TEMPLATE_NAME=$(jq -r '.name' deid-template.json)
export INSPECT_TEMPLATE_NAME=$(jq -r '.name' inspect-template.json)
export REID_TEMPLATE_NAME=$(jq -r '.name' reid-template.json)

export ENV_FILE="set_env.sh"
# trigger the dataflow pipeline
echo '#!/bin/bash'>$ENV_FILE
echo "export PROJECT_ID=$PROJECT_ID">>$ENV_FILE
echo "export INSPECT_TEMPLATE_NAME=$INSPECT_TEMPLATE_NAME">>$ENV_FILE
echo "export DEID_TEMPLATE_NAME=$DEID_TEMPLATE_NAME">>$ENV_FILE
echo "export REID_TEMPLATE_NAME=$REID_TEMPLATE_NAME">>$ENV_FILE
echo "export SERVICE_ACCOUNT_EMAIL=$SERVICE_ACCOUNT_EMAIL">>$ENV_FILE
echo "export DATA_STORAGE_BUCKET=$DATA_STORAGE_BUCKET">>$ENV_FILE
echo "export GCS_NOTIFICATION_TOPIC=$GCS_NOTIFICATION_TOPIC">>$ENV_FILE
echo "export BQ_DATASET_NAME=$BQ_DATASET_NAME">>$ENV_FILE
echo "export REGION=$REGION">>$ENV_FILE
