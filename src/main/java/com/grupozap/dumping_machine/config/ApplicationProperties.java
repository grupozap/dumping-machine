package com.grupozap.dumping_machine.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@ConfigurationProperties(prefix = "dumping-machine")
public class ApplicationProperties {
    private String bootstrapServer;
    private String schemaRegistryUrl;
    private String groupId;
    private List<TopicProperties> topics;

    public List<TopicProperties> getTopics() {
        return topics;
    }

    public void setTopics(List<TopicProperties> topics) {
        this.topics = topics;
    }

    public String getBootstrapServer() {
        return bootstrapServer;
    }

    public void setBootstrapServer(String bootstrapServer) {
        this.bootstrapServer = bootstrapServer;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }
}
