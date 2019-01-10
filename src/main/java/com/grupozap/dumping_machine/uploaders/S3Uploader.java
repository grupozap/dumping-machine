package com.grupozap.dumping_machine.uploaders;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;

public class S3Uploader {
    String clientRegion;
    String bucketName;
    AmazonS3 s3Client;

    public S3Uploader() {
        this.clientRegion = "us-east-1";
        this.bucketName = "grupozap-dumping-machine-dev";
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

    public void upload(String remotePath, String filename) {
        this.s3Client.putObject(new PutObjectRequest(bucketName, remotePath, new File(filename)));
    }
}
