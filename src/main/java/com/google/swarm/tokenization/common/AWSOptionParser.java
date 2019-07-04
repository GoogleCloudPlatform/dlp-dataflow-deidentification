/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.swarm.tokenization.common;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class AWSOptionParser {

	private static final String AWS_DEFAULT_REGION = "us-east-1";
	private static final String AWS_S3_PREFIX = "s3";

	public static void formatOptions(S3ImportOptions options) {

		ClientConfiguration configuration = new ClientConfiguration().withMaxConnections(options.getMaxConnections())
				.withConnectionTimeout(options.getConnectionTimeout()).withSocketTimeout(options.getSocketTimeout());

		options.setClientConfiguration(configuration);
		//if (options.getS3BucketUrl()!=null && options.getS3BucketUrl().isAccessible()) {
			if (options.getS3BucketUrl().get().toLowerCase().startsWith(AWS_S3_PREFIX)) {
				setAwsCredentials(options);
			}

			if (options.getAwsRegion() == null) {
				setAwsDefaultRegion(options);
			}
		}
		
	//}

	private static void setAwsCredentials(S3ImportOptions options) {
		options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(
				new BasicAWSCredentials(options.getAwsAccessKey().get(), options.getAwsSecretKey().get())));
	}

	private static void setAwsDefaultRegion(S3ImportOptions options) {
		if (options.getAwsRegion() == null) {
			options.setAwsRegion(AWS_DEFAULT_REGION);
		}
	}
}
