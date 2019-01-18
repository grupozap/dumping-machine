package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.partitioners.HourlyBasedPartitioner;
import com.grupozap.dumping_machine.uploaders.Uploader;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TopicStreamer implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(TopicStreamer.class);

    private final String topic;
    private final long poolTimeout;
    private final Uploader uploader;
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final long partitionForget;

    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, Uploader uploader, String topic, long poolTimeout, long partitionForget) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.uploader = uploader;
        this.topic = topic;
        this.poolTimeout = poolTimeout;
        this.partitionForget = partitionForget;
    }

    @Override
    public void run() {
        ConsumerRecords<String, GenericRecord> records;
        HourlyBasedPartitioner hourlyBasedPartitioner = new HourlyBasedPartitioner(this.topic, this.uploader, this.partitionForget);
        KafkaConsumer consumer = getConsumer();

        consumer.subscribe(Arrays.asList(this.topic));

        try {
            while (true) {
                records = consumer.poll(this.poolTimeout);

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    if (record.value() != null) {
                        hourlyBasedPartitioner.consume(new AvroExtendedMessage(record));
                    }
                }

                // Flush closed partitions
                consumer.commitSync(hourlyBasedPartitioner.getClosedPartitions());
            }
        } finally {
            logger.error("Topic: " + this.topic + " - Closing consumer");
            consumer.unsubscribe();
            consumer.close();
        }
    }

    private KafkaConsumer<String, GenericRecord> getConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);

        return new KafkaConsumer<>(props);
    }
}