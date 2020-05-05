package com.grupozap.dumping_machine.config;

public class TopicProperties {
    private String name;
    private String type;
    private String uploaderClass;
    private String bucketName;
    private String bucketRegion;
    private String hdfsPath;
    private String topicPath;
    private String coreSitePath;
    private String hdfsSitePath;
    private Long poolTimeout;
    private Long partitionForget;
    private HiveProperties hive;
    private AWSGlueProperties awsGlue;

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
            return 100L;
        } else {
            return poolTimeout;
        }
    }

    public void setPoolTimeout(Long poolTimeout) {
        this.poolTimeout = poolTimeout;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHdfsPath() {
        return hdfsPath;
    }

    public void setHdfsPath(String hdfsPath) {
        this.hdfsPath = hdfsPath;
    }

    public String getCoreSitePath() {
        return coreSitePath;
    }

    public void setCoreSitePath(String coreSitePath) {
        this.coreSitePath = coreSitePath;
    }

    public String getHdfsSitePath() {
        return hdfsSitePath;
    }

    public void setHdfsSitePath(String hdfsSitePath) {
        this.hdfsSitePath = hdfsSitePath;
    }

    public String getTopicPath() {
        return topicPath;
    }

    public void setTopicPath(String topicPath) {
        this.topicPath = topicPath;
    }

    public HiveProperties getHive() {
        return hive;
    }

    public void setHive(HiveProperties hive) {
        this.hive = hive;
    }

    public AWSGlueProperties getAwsGlue() { return awsGlue; }

    public void setAwsGlue(AWSGlueProperties awsGlue) { this.awsGlue = awsGlue; }
}
