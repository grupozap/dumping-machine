package com.grupozap.dumping_machine.config;

public class TopicProperties {
    private String name;
    private String uploaderClass;
    private String bucketName;
    private String bucketRegion;
    private Long poolTimeout;
    private Long partitionForget;

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

    public Long getPartitionForget() {
        return partitionForget;
    }

    public void setPartitionForget(Long partitionForget) {
        this.partitionForget = partitionForget;
    }

    public Long getPoolTimeout() {
        if(poolTimeout == null) {
            return Long.valueOf(100);
        } else {
            return poolTimeout;
        }
    }

    public void setPoolTimeout(Long poolTimeout) {
        this.poolTimeout = poolTimeout;
    }
}
