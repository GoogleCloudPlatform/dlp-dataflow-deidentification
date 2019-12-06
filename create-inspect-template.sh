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
INSPECT_CONFIG="@gcs-s3-inspect-config.json"
INSPECT_TEMPLATE_OUTPUT="inspect-template.json"
DLP_API_ROOT_URL="https://dlp.googleapis.com"
INSPECT_TEMPLATE_API="${DLP_API_ROOT_URL}/v2/projects/${PROJECT_ID}/inspectTemplates"
curl -X POST -H "Content-Type: application/json" \
 -H "Authorization: Bearer ${API_KEY}" \
 "${INSPECT_TEMPLATE_API}"`` \
 -d "${INSPECT_CONFIG}"\
 -o "${INSPECT_TEMPLATE_OUTPUT}"

