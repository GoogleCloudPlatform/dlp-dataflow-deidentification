package com.google.swarm.tokenization.common;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

public class AWSOptionParser {

	  private static final String AWS_DEFAULT_REGION = "us-east-1";
	  private static final String AWS_S3_PREFIX = "s3";

	  public static void formatOptions(S3ImportOptions options) {
	    if (options.getBucketUrl().toLowerCase().startsWith(AWS_S3_PREFIX)) {
	      setAwsCredentials(options);
	    }

	    if (options.getAwsRegion() == null) {
	      setAwsDefaultRegion(options);
	    }
	  }

	  private static void setAwsCredentials(S3ImportOptions options) {
	    options.setAwsCredentialsProvider(
	        new AWSStaticCredentialsProvider(
	            new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())));
	  }

	  private static void setAwsDefaultRegion(S3ImportOptions options) {
	    if (options.getAwsRegion() == null) {
	      options.setAwsRegion(AWS_DEFAULT_REGION);
	    }
	  }
	}
