package com.grupozap.dumping_machine.streamers;

import com.grupozap.dumping_machine.config.ApplicationProperties;
import com.grupozap.dumping_machine.config.TopicProperties;
import com.grupozap.dumping_machine.deserializers.RecordType;
import com.grupozap.dumping_machine.metastore.*;
import com.grupozap.dumping_machine.streamers.kafka.TopicStreamer;
import com.grupozap.dumping_machine.uploaders.HDFSUploader;
import com.grupozap.dumping_machine.uploaders.S3Uploader;
import com.grupozap.dumping_machine.uploaders.Uploader;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaStreamer {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final Integer sessionTimeout;
    private String metadataPropertyName;
    private final List<TopicProperties> topicProperties;
    private final String partitionPattern;

    private final ExecutorService pool;

    public KafkaStreamer(ApplicationProperties applicationProperties) {
        this.bootstrapServers = applicationProperties.getBootstrapServers();
        this.groupId = applicationProperties.getGroupId();
        this.schemaRegistryUrl = applicationProperties.getSchemaRegistryUrl();
        this.topicProperties = applicationProperties.getTopics();
        this.sessionTimeout = applicationProperties.getSessionTimeout();
        this.pool = Executors.newFixedThreadPool(topicProperties.size());
        this.metadataPropertyName = applicationProperties.getMetadataPropertyName();
        this.partitionPattern = applicationProperties.getPartitionPattern();
    }

    private MetastoreService buildMetastoreService(TopicProperties topicProperty) throws Exception {
        MetastoreService ms = null;

        if(topicProperty.getHive() != null && topicProperty.getAwsGlue() != null) {
            throw new Exception("Invalid setting. Use only 1 metastore");
        }

        HashMap<RecordType, String> tables = new HashMap<>();

        if(topicProperty.getHive() != null) {
            tables.put(RecordType.RECORD, topicProperty.getHive().getRecordTable());
            tables.put(RecordType.ERROR, topicProperty.getHive().getErrorTable());
            tables.put(RecordType.TOMBSTONE, topicProperty.getHive().getTombstoneTable());
            ms = new MetastoreService(tables, new HiveClient(topicProperty.getHive().getUrl()), new AvroToHive());

        }

        if(topicProperty.getAwsGlue() != null) {
            tables.put(RecordType.RECORD, topicProperty.getAwsGlue().getRecordTable());
            tables.put(RecordType.ERROR, topicProperty.getAwsGlue().getErrorTable());
            tables.put(RecordType.TOMBSTONE, topicProperty.getAwsGlue().getTombstoneTable());
            ms = new MetastoreService(tables, new AWSGlueClient(topicProperty.getAwsGlue().getRegion()), new AvroToAWSGlue());
        }

        return ms;

    }

    public void run() throws Exception {

        for(TopicProperties topicProperty : topicProperties) {
            System.out.println(topicProperty.getAwsGlue().getRecordTable());
            Uploader uploader;
            MetastoreService ms = buildMetastoreService(topicProperty);

            if(topicProperty.getType().equals("HDFSUploader")) {
                uploader = new HDFSUploader(topicProperty.getHdfsPath(), topicProperty.getCoreSitePath(), topicProperty.getHdfsSitePath(), topicProperty.getTopicPath());
            } else {
                uploader = new S3Uploader(topicProperty.getBucketName(), topicProperty.getBucketRegion());
            }

            TopicStreamer topicStreamer = new TopicStreamer(this.bootstrapServers, this.groupId, this.schemaRegistryUrl, this.sessionTimeout, uploader, topicProperty.getName(), topicProperty.getPoolTimeout(), topicProperty.getPartitionForget(),  this.metadataPropertyName, this.partitionPattern, ms);

            this.pool.execute(topicStreamer);
        }
    }
}
