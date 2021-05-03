/*
This is a terraform script for setting up the prerequisite GCP products for running the DLP demo. The script assumes that the user has authenticated with GCP by setting the environmental variable GOOGLE_APPLICATION_CREDENTIALS as seen in https://cloud.google.com/docs/authentication/production. Set your variables in the local section of this script. Note that if you are using your own key ring/encryption set variable `create_key_ring`=false.
*/

locals={
    gcp_project   = "" #GCP Project Name
    gcp_region = "" #GCP Project Region
    gcp_zone ="" #GCP Project Zone
    gcs_bucket_name = "" #New GCS Bucket to be created for file
    kms_key_name = "" #KMS Key Name (can be created or will be created)
    key_ring = "" #KMS Key Name (can be created or will be created)
    key_region = "" #Region where KMS key ring is located (can be created or will be created)
    wrapped_key = "" #Wrapped key from KMS leave blank if create_key_ring = true
    create_key_ring = "false"
}

provider "google" {
  project = "${local.gcp_project}"
  region  = "${local.gcp_region}"
  zone    = "${local.gcp_zone}"
}

resource "google_storage_bucket" "cc_store" {
  name     = "${local.gcs_bucket_name}"
  storage_class =  "MULTI_REGIONAL"
  
  website {
    not_found_page   = "404.html"
  }
}

resource "null_resource" "download_sample_cc_into_gcs"{
  provisioner "local-exec" {
    command = <<EOF
    echo ${local.gcp_project} > test.txt 
    curl http://eforexcel.com/wp/wp-content/uploads/2017/07/1500000%20CC%20Records.zip > cc_records.zip
    unzip cc_records.zip
    rm cc_records.zip 
    mv 1500000\ CC\ Records.csv cc_records.csv
    gsutil cp cc_records.csv gs://${google_storage_bucket.cc_store.name}
    rm cc_records.csv
    EOF
    }
}



resource "null_resource" "deinspection_template_setup"{
  provisioner "local-exec" {
    command = <<EOF
    if [ -f wrapped_key.txt ] && [ ${null_resource.create_kms_wrapped_key.count}=1 ]; then
      wrapped_key=$(cat wrapped_key.txt)
    else
      wrapped_key=${local.wrapped_key}
    fi

    echo $wrapped_key

    curl https://tt-dlp.googleapis.com/v2/projects/${local.gcp_project}/deidentifyTemplates -H "Authorization: Bearer $(gcloud auth application-default print-access-token)" \
    -H "Content-Type: application/json" \
    -d '{"deidentifyTemplate": {"deidentifyConfig": {"recordTransformations": {"fieldTransformations": [{"fields": [{"name": "card_number"}, {"name": "cvvcvv2"}, {"name": "card_pin"}], "primitiveTransformation": {"cryptoReplaceFfxFpeConfig": {"cryptoKey": {"kmsWrapped": {"cryptoKeyName": "projects/${local.gcp_project}/locations/${local.key_region}/keyRings/${local.key_ring}/cryptoKeys/${local.kms_key_name}", "wrappedKey": "'$wrapped_key'"}}, "commonAlphabet": "ALPHA_NUMERIC"}}}]}}}, "templateId": "15"}'
    EOF
    }
}

resource "google_bigquery_dataset" "default" {
  dataset_id                  = "dlp_demo"
  friendly_name               = "dlp_demo"
  description                 = "This is the BQ dataset for running the dlp demo"
  location                    = "US"
  default_table_expiration_ms = 3600000
}

resource "google_bigquery_table" "default" {
  dataset_id = "${google_bigquery_dataset.default.dataset_id}"
  table_id   = "credit_cards"
  schema = "${file("bq_schema.json")}"
}

resource "google_kms_key_ring" "create_kms_ring" {
  count =   "${local.create_key_ring == true ? 1 : 0}"
  name     = "${local.key_ring}"
  location = "${local.key_region}"
}

resource "google_kms_crypto_key" "create_kms_key" {
  count  = "${google_kms_key_ring.create_kms_ring.count}"
  name =  "${local.kms_key_name}"
  key_ring = "${google_kms_key_ring.create_kms_ring.self_link}"
}


resource "null_resource" "create_kms_wrapped_key"{
  count = "${google_kms_crypto_key.create_kms_key.count}"
  provisioner "local-exec" {
  command=<<EOF
  rm original_key.txt
  rm wrapped_key.txt
  python -c "import os,base64; key=os.urandom(32); encoded_key = base64.b64encode(key).decode('utf-8'); print(encoded_key)" >> original_key.txt
  original_key="$(cat original_key.txt)"
  gcloud kms keys add-iam-policy-binding ${var.kms_key_name} --location global --keyring ${var.key_ring} --member allAuthenticatedUsers --role roles/cloudkms.cryptoKeyEncrypterDecrypter
  curl -s -X POST "https://cloudkms.googleapis.com/v1/projects/${local.gcp_project}/locations/${local.key_region}/keyRings/${local.key_ring}/cryptoKeys/${local.kms_key_name}:encrypt"  -d '{"plaintext":"'$original_key'"}'  -H "Authorization:Bearer $(gcloud auth application-default print-access-token)"  -H "Content-Type:application/json" | python -c "import sys, json; print(json.load(sys.stdin)['ciphertext'])" >> wrapped_key.txt
  EOF
  }


}

