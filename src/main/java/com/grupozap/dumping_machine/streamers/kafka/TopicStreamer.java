package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.partitioners.HourlyBasedPartitioner;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;

import java.util.*;

public class TopicStreamer implements Runnable {
    private final String topic;
    private final long poolSize;
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;

    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, String topic) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.topic = topic;
        this.poolSize = 100;
    }

    @Override
    public void run() {
        ConsumerRecords<String, GenericRecord> records;
        HourlyBasedPartitioner hourlyBasedPartitioner = new HourlyBasedPartitioner(this.topic);
        KafkaConsumer consumer = getConsumer();

        try (consumer) {
            consumer.subscribe(Arrays.asList(this.topic));

            while (true) {
                records = consumer.poll(this.poolSize);

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    if (record.value() != null) {
                        hourlyBasedPartitioner.consume(record);
                    }
                }

                // Flush closed partitions
                consumer.commitSync(hourlyBasedPartitioner.getClosedPartitions());
            }
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
        props.put("schema.registry.url", this.schemaRegistryUrl);

        return new KafkaConsumer<>(props);
    }
}