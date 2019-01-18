package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.partitioners.HourlyBasedPartitioner;
import com.grupozap.dumping_machine.partitioners.PartitionInfo;
import com.grupozap.dumping_machine.uploaders.Uploader;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.*;

public class TopicStreamer implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(TopicStreamer.class);

    private final String topic;
    private final long poolTimeout;
    private final Uploader uploader;
    private final String bootstrapServers;
    private final String groupId;
    private final String schemaRegistryUrl;
    private final int sessionTimeout;
    private final long partitionForget;

    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, int sessionTimeout, Uploader uploader, String topic, long poolTimeout, long partitionForget) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.sessionTimeout = sessionTimeout;
        this.uploader = uploader;
        this.topic = topic;
        this.poolTimeout = poolTimeout;
        this.partitionForget = partitionForget;
    }

    @Override
    public void run() {
        ConsumerRecords<String, GenericRecord> records;
        KafkaConsumer consumer = getConsumer();
        HourlyBasedPartitioner hourlyBasedPartitioner = new HourlyBasedPartitioner(this.topic, this.uploader, this.partitionForget);
        TopicConsumerRebalanceListener topicConsumerRebalanceListener = new TopicConsumerRebalanceListener(consumer, this.topic, hourlyBasedPartitioner);

        consumer.subscribe(Arrays.asList(this.topic), topicConsumerRebalanceListener);

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
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeout);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);

        return new KafkaConsumer<>(props);
    }

    private static class TopicConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final Logger logger = LoggerFactory.getLogger(TopicConsumerRebalanceListener.class);

        private final String topic;
        private Consumer<?, ?> consumer;
        private final HourlyBasedPartitioner hourlyBasedPartitioner;

        public TopicConsumerRebalanceListener(Consumer<?, ?> consumer, String topic, HourlyBasedPartitioner hourlyBasedPartitioner) {
            this.consumer = consumer;
            this.topic = topic;
            this.hourlyBasedPartitioner = hourlyBasedPartitioner;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            int partitionsCount = 0;

            for(Integer partition : hourlyBasedPartitioner.getPartitionInfos().keySet()) {
                if(partitions.contains(partition)) {
                    partitionsCount++;
                }
            }

            if(partitionsCount == hourlyBasedPartitioner.getPartitionInfos().keySet().size()) { // If this is just a session timeout
                for(Map.Entry<Integer, PartitionInfo> entry : hourlyBasedPartitioner.getPartitionInfos().entrySet()) {
                    logger.info("Topic: " + this.topic + " - Seeking partition " + entry.getKey() + " to offset " + entry.getValue().getOffset());

                    consumer.seek(new TopicPartition(this.topic, entry.getKey()), entry.getValue().getOffset());
                }
            } else { // If this is a rebalancing
                logger.info("Topic: " + this.topic + " - Cleaning for rebalance");

                hourlyBasedPartitioner.clearPartitions();
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Topic: " + this.topic + " - Revoking partitions");
        }
    }
}