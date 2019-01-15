package com.grupozap.dumping_machine.config;

public class TopicProperties {
    private String name;
    private String uploaderClass;
    private String bucketName;
    private String bucketRegion;

    public String getUploaderClass() {
        return uploaderClass;
    }

    public void setUploaderClass(String uploaderClass) {
        this.uploaderClass = uploaderClass;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getBucketRegion() {
        return bucketRegion;
    }

    public void setBucketRegion(String bucketRegion) {
        this.bucketRegion = bucketRegion;
    }
}
