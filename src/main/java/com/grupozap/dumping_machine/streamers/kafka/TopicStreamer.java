package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.uploaders.S3Uploader;
import com.grupozap.dumping_machine.writers.AvroParquetRecordWriter;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class TopicStreamer implements Runnable {
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final String topic;
    private final long poolSize;
    private final long uploadMillis;
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private KafkaConsumer consumer;

    private final int blockSize = 256 * 1024 * 1024;
    private final int pageSize = 64 * 1024;

    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.poolSize = 100;
        this.uploadMillis = 10000;
        this.avroParquetRecordWriter = null;

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
        consumer.subscribe(Arrays.asList(this.topic));

        while (true) {
            if(avroParquetRecordWriter != null) {
                if((System.currentTimeMillis() - avroParquetRecordWriter.getCreatedAt()) > uploadMillis) {
                    this.closeFile();
                    this.uploadFile();

                    avroParquetRecordWriter = null;
                }

            }

            ConsumerRecords<String, GenericRecord> records = consumer.poll(this.poolSize);

            for (ConsumerRecord<String, GenericRecord> record : records) {
                this.storeRecord(record);

//                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    private String generateFilename() {
        List<String> offsets = offsets().stream().map(Object::toString).collect(Collectors.toList());
        List<String> partitions = partitions().stream().map(Object::toString).collect(Collectors.toList());

        String offsetAndPartitions = String.format("%s%s", topic, String.join("", offsets), String.join("", partitions));

        return DigestUtils.md5Hex(offsetAndPartitions);
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

    private void storeRecord(ConsumerRecord<String, GenericRecord> record) {
        if(avroParquetRecordWriter == null) {
            try {
                avroParquetRecordWriter = new AvroParquetRecordWriter(record.value().getSchema(), generateFilename(), blockSize, pageSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if(record != null) {
            avroParquetRecordWriter.write(record.value());
        }
    }

    private void closeFile() {
        if (avroParquetRecordWriter != null) {
            avroParquetRecordWriter.close();
        }
    }

    private void uploadFile() {
        if (avroParquetRecordWriter != null) {
            new S3Uploader().upload(this.avroParquetRecordWriter.getFilename());
        }
    }
}