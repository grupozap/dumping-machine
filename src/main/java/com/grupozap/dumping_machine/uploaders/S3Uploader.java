package com.grupozap.dumping_machine.uploaders;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.grupozap.dumping_machine.partitioners.HourlyBasedPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class S3Uploader implements Uploader {
    private final String bucketName;
    private final String bucketRegion;
    private final static String TYPE = "s3a";

    private final Logger logger = LoggerFactory.getLogger(HourlyBasedPartitioner.class);

    public S3Uploader(String bucketName, String bucketRegion) {
        this.bucketName = bucketName;
        this.bucketRegion = bucketRegion;
    }

    public void upload(String remotePath, String filename) {
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(this.bucketRegion)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();
        String bucketName = this.bucketName;
        String bucketPath = "";
        if (bucketName.indexOf('/') >= 0) {
            bucketPath = bucketName.substring(bucketName.indexOf('/') + 1);
            bucketName = bucketName.substring(0, bucketName.indexOf('/'));
        }
        if (bucketPath.matches(".*[^/]$")) {
            bucketPath = bucketPath + "/";
        }
        logger.info("putObject BucketName:[" + bucketName + "],  bucketPath: [" + bucketPath + "],  remotePath: [" + remotePath + "], filename: [" + filename + "]");

        s3Client.putObject(new PutObjectRequest(bucketName, bucketPath + remotePath, new File(filename)).withCannedAcl(CannedAccessControlList.BucketOwnerFullControl));
    }
    public String getServerPath() {
        return TYPE + "://" + bucketName;
    }
}
