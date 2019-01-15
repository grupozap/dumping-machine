package com.grupozap.dumping_machine.streamers;

import com.grupozap.dumping_machine.config.ApplicationProperties;
import com.grupozap.dumping_machine.streamers.kafka.TopicStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
class KafkaStreamer implements ApplicationListener<ApplicationReadyEvent> {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final List<ApplicationProperties.TopicProperties> topics;

    private final ExecutorService pool;

    @Autowired
    public KafkaStreamer(ApplicationProperties applicationProperties) {
        this.bootstrapServers = applicationProperties.getBootstrapServers();
        this.groupId = applicationProperties.getGroupId();
        this.schemaRegistryUrl = applicationProperties.getSchemaRegistryUrl();
        this.topics = applicationProperties.getTopics();
        this.pool = Executors.newFixedThreadPool(topics.size());
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        this.topics.forEach((topic) -> this.pool.execute(new TopicStreamer(this.bootstrapServers, this.groupId, this.schemaRegistryUrl, topic.getName())));
    }
}
