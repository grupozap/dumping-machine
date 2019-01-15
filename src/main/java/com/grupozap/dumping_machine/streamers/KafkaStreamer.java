package com.grupozap.dumping_machine.streamers;

import com.grupozap.dumping_machine.config.ApplicationProperties;
import com.grupozap.dumping_machine.config.TopicProperties;
import com.grupozap.dumping_machine.streamers.kafka.TopicStreamer;
import com.grupozap.dumping_machine.uploaders.S3Uploader;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaStreamer {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final List<TopicProperties> topicProperties;

    private final ExecutorService pool;

    public KafkaStreamer(ApplicationProperties applicationProperties) {
        this.bootstrapServers = applicationProperties.getBootstrapServers();
        this.groupId = applicationProperties.getGroupId();
        this.schemaRegistryUrl = applicationProperties.getSchemaRegistryUrl();
        this.topicProperties = applicationProperties.getTopics();
        this.pool = Executors.newFixedThreadPool(topicProperties.size());
    }

    public void run() {
        for(TopicProperties topicProperty : topicProperties) {
            S3Uploader s3Uploader = new S3Uploader(topicProperty.getBucketName(), topicProperty.getBucketRegion());
            TopicStreamer topicStreamer = new TopicStreamer(this.bootstrapServers, this.groupId, this.schemaRegistryUrl, s3Uploader, topicProperty.getName());

            this.pool.execute(topicStreamer);
        }
    }
}
