package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.partitioners.TimeBasedPartitioner;
import com.grupozap.dumping_machine.writers.AvroParquetRecordWriter;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class TopicStreamer implements Runnable {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final String topic;
    private final String localPath;
    private final String remotePath;
    private final long poolSize;
    private final long uploadMillis;
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private TimeBasedPartitioner timeBasedPartitioner;
    private KafkaConsumer consumer;



    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.poolSize = 100;
        this.uploadMillis = 10000;
        this.avroParquetRecordWriter = null;
        this.localPath = "/tmp/";
        this.remotePath = "tmp/";

        Properties props = new Properties();

        props.put("bootstrap.servers", this.bootstrapServers);
        props.put("group.id", this.groupId);
        props.put("enable.auto.commit", "false");
        props.put("schema.registry.url", this.schemaRegistryUrl);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", KafkaAvroDeserializer.class);

        this.consumer = new KafkaConsumer<String, GenericRecord>(props);
    }

    @Override
    public void run() {
        boolean close = false;

        timeBasedPartitioner = new TimeBasedPartitioner(topic);
        consumer.subscribe(Arrays.asList(this.topic));

        while (true) {
            timeBasedPartitioner.closeOldPartitions();

            if(close == true) { consumer.commitSync(); }

            ConsumerRecords<String, GenericRecord> records = consumer.poll(this.poolSize);

            for (ConsumerRecord<String, GenericRecord> record : records) {
                timeBasedPartitioner.write(record);
            }
        }
    }

    private List<Long> offsets() {
        List<Long> offsets = new ArrayList<>();

        consumer.assignment().forEach( (topicPartition) -> {
            OffsetAndMetadata commit = consumer.committed((TopicPartition) topicPartition);

            if(commit == null) {
                offsets.add((long) 0);
            } else {
                offsets.add(commit.offset());
            }
        });

        return offsets;
    }

    private List<Integer> partitions() {
        List<Integer> partitions = new ArrayList<>();

        consumer.assignment().forEach( (topicPartition) -> partitions.add(((TopicPartition) topicPartition).partition()) );

        return partitions;
    }
}