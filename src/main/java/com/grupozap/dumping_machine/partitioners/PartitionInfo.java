package com.grupozap.dumping_machine.partitioners;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class PartitionInfo {
    private final String topic;
    private final int partition;
    private long lastOffset;

    public PartitionInfo(String topic, int partition, long lastOffset) {
        this.topic = topic;
        this.partition = partition;
        this.lastOffset = lastOffset;
    }

    public int getPartition() {
        return partition;
    }

    public long getLastOffset() {
        return lastOffset;
    }

    public void setLastOffset(long lastOffset) {
        this.lastOffset = lastOffset;
    }

    public TopicPartition getTopicPartition() {
        return new TopicPartition(this.topic, this.getPartition());
    }

    public OffsetAndMetadata getNextOffsetAndMetadata() {
        return new OffsetAndMetadata(this.getLastOffset() + 1);
    }
}
