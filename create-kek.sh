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

set -x
PROJECT_ID=$1
KEY_RING_NAME=$2
KEY_NAME=$3
TEK=$4
KEK=$5
API_KEY=$6
API_ROOT_URL="https://cloudkms.googleapis.com"
KEK_API="${API_ROOT_URL}/v1/projects/${PROJECT_ID}/locations/global/keyRings/${KEY_RING_NAME}/cryptoKeys/${KEY_NAME}:encrypt"
curl -X POST -H "Content-Type: application/json" \
    -H "Authorization: Bearer ${API_KEY}" \
    "${KEK_API}"$() \
    -d '{"plaintext":"'${TEK}'"}' \
    -o "${KEK}" 
