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
export PROJECT_ID=$(gcloud config get-value project)
gcloud services enable dlp.googleapis.com
gcloud services enable cloudkms.googleapis.com
gcloud services enable bigquery
gcloud services enable storage_component
gcloud services enable dataflow
gcloud services enable cloudbuild.googleapis.com
export AWS_ACCESS_KEY="<access_key>"
export AWS_SECRET_KEY="<secret_key>"
export S3_BUCKET_URL="s3://<path>"
export GCS_BUCKET_URL="gs://<path>"
export AWS_REGION="<region>"
export BQ_DATASET="dlp_inspection"

gcloud builds submit . --config dlp-demo-s3-gcs-inspect.yaml --substitutions _AWS_ACCESS_KEY=$AWS_ACCESS_KEY,_API_KEY=$(gcloud auth print-access-token),_AWS_SECRET_KEY=$AWS_SECRET_KEY,_S3_BUCKET_URL=$S3_BUCKET_URL,_GCS_BUCKET_URL=$GCS_BUCKET_URL,_AWS_REGION=$AWS_REGION,_BQ_DATASET=$BQ_DATASET
