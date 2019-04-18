package com.grupozap.dumping_machine.uploaders;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;

import java.io.File;

public class S3Uploader implements Uploader {
    private final String bucketName;
    private final String bucketRegion;

    public S3Uploader(String bucketName, String bucketRegion) {
        this.bucketName = bucketName;
        this.bucketRegion = bucketRegion;
    }

    public void upload(String remotePath, String filename) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(this.bucketRegion)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        s3Client.putObject(new PutObjectRequest(this.bucketName, remotePath, new File(filename)));
    }
}
