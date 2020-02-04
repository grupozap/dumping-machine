package com.grupozap.dumping_machine.streamers;

import com.grupozap.dumping_machine.config.ApplicationProperties;
import com.grupozap.dumping_machine.config.TopicProperties;
import com.grupozap.dumping_machine.deserializers.RecordType;
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

    private final ExecutorService pool;

    public KafkaStreamer(ApplicationProperties applicationProperties) {
        this.bootstrapServers = applicationProperties.getBootstrapServers();
        this.groupId = applicationProperties.getGroupId();
        this.schemaRegistryUrl = applicationProperties.getSchemaRegistryUrl();
        this.topicProperties = applicationProperties.getTopics();
        this.sessionTimeout = applicationProperties.getSessionTimeout();
        this.pool = Executors.newFixedThreadPool(topicProperties.size());
        this.metadataPropertyName = applicationProperties.getMetadataPropertyName();
    }

    public void run() {

        for(TopicProperties topicProperty : topicProperties) {
            Uploader uploader;
            HashMap<RecordType, String> hiveTables = new HashMap<>();
            String metaStoreUris = null;

            if(topicProperty.getHive() != null) {
                metaStoreUris = topicProperty.getHive().getUrl();
                hiveTables.put(RecordType.RECORD, topicProperty.getHive().getRecordTable());
                hiveTables.put(RecordType.ERROR, topicProperty.getHive().getErrorTable());
                hiveTables.put(RecordType.TOMBSTONE, topicProperty.getHive().getTombstoneTable());
            }

            if(topicProperty.getType().equals("HDFSUploader")) {
                uploader = new HDFSUploader(topicProperty.getHdfsPath(), topicProperty.getCoreSitePath(), topicProperty.getHdfsSitePath(), topicProperty.getTopicPath());
            } else {
                uploader = new S3Uploader(topicProperty.getBucketName(), topicProperty.getBucketRegion());
            }

            TopicStreamer topicStreamer = new TopicStreamer(this.bootstrapServers, this.groupId, this.schemaRegistryUrl, this.sessionTimeout, uploader, topicProperty.getName(), topicProperty.getPoolTimeout(), topicProperty.getPartitionForget(), metaStoreUris, hiveTables, this.metadataPropertyName);

            this.pool.execute(topicStreamer);
        }
    }
}
