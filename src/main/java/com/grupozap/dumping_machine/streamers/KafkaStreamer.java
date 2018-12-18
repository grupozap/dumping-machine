package com.grupozap.dumping_machine.streamers;

import com.grupozap.dumping_machine.streamers.kafka.TopicStreamer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public class KafkaStreamer implements ApplicationListener<ApplicationReadyEvent> {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final List<String> topics;

    private final ExecutorService pool;

    @Autowired
    public KafkaStreamer(
            @Value("${dumping-machine.bootstrap-servers}") String bootstrapServers,
            @Value("${dumping-machine.group-id}") String groupId,
            @Value("${dumping-machine.schema-registry-url}") String schemaRegistryUrl,
            @Value("${dumping-machine.topics}") List<String> topics) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topics = topics;
        this.pool = Executors.newFixedThreadPool(topics.size());
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        this.topics.forEach((topic) -> this.pool.execute(new TopicStreamer(bootstrapServers, groupId, schemaRegistryUrl, topic)));
    }
}
