package com.grupozap.dumping_machine.streamers.kafka;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.metastore.MetastoreService;
import com.grupozap.dumping_machine.partitioners.TimeBasedPartitioner;
import com.grupozap.dumping_machine.partitioners.PartitionInfo;
import com.grupozap.dumping_machine.uploaders.Uploader;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
    private final String metadataPropertyName;
    private final String partitionPattern;
    private final MetastoreService metastoreService;

    public TopicStreamer(String bootstrapServers, String groupId, String schemaRegistryUrl, int sessionTimeout, Uploader uploader, String topic, long poolTimeout, long partitionForget, String metadataPropertyName, String partitionPattern, MetastoreService metastoreService) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.sessionTimeout = sessionTimeout;
        this.uploader = uploader;
        this.topic = topic;
        this.poolTimeout = poolTimeout;
        this.partitionForget = partitionForget;
        this.metadataPropertyName = metadataPropertyName;
        this.partitionPattern = partitionPattern;
        this.metastoreService = metastoreService;
    }

    @Override
    public void run() {
        ConsumerRecords<String, GenericRecord> records;
        KafkaConsumer consumer = getConsumer();
        TimeBasedPartitioner timeBasedPartitioner = new TimeBasedPartitioner(this.topic, this.uploader, this.partitionForget, this.partitionPattern, this.metastoreService);
        TopicConsumerRebalanceListener topicConsumerRebalanceListener = new TopicConsumerRebalanceListener(consumer, this.topic, timeBasedPartitioner);

        consumer.subscribe(Arrays.asList(this.topic), topicConsumerRebalanceListener);
        try {
            while (true) {
                records = consumer.poll(this.poolTimeout);
                logger.trace("Topic: " + this.topic + " - Consuming " + records.count() + " records");

                for (ConsumerRecord<String, GenericRecord> record : records) {
                    timeBasedPartitioner.consume(new AvroExtendedMessage(record, this.metadataPropertyName));
                }

                // Flush closed partitions
                consumer.commitSync(timeBasedPartitioner.commitWriters());
            }
        } catch (Exception e) {
            logger.error("Topic: " + this.topic + " - Error on consumption. Message: " + e.getMessage(), e);
        } finally {
            logger.info("Topic: " + this.topic + " - Closing consumer");
            consumer.unsubscribe();
            consumer.close();
            this.metastoreService.close();
        }
    }

    private KafkaConsumer<String, GenericRecord> getConsumer() {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.grupozap.dumping_machine.deserializers.AvroSchemaRegistryDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, this.sessionTimeout);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, this.schemaRegistryUrl);

        return new KafkaConsumer<>(props);
    }

    private static class TopicConsumerRebalanceListener implements ConsumerRebalanceListener {
        private final Logger logger = LoggerFactory.getLogger(TopicConsumerRebalanceListener.class);

        private final String topic;
        private final Consumer<?, ?> consumer;
        private final TimeBasedPartitioner timeBasedPartitioner;

        TopicConsumerRebalanceListener(Consumer<?, ?> consumer, String topic, TimeBasedPartitioner timeBasedPartitioner) {
            this.consumer = consumer;
            this.topic = topic;
            this.timeBasedPartitioner = timeBasedPartitioner;
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            int partitionsCount = 0;

            for(PartitionInfo partitionInfo : timeBasedPartitioner.getPartitionInfos()) {
                for(TopicPartition topicPartition : partitions) {
                    if(topicPartition.partition() == partitionInfo.getPartition()) {
                        partitionsCount++;
                    }
                }
            }

            if(partitionsCount == timeBasedPartitioner.getPartitionInfos().size()) { // If this is just a session timeout
                for(PartitionInfo partitionInfo : timeBasedPartitioner.getPartitionInfos()) {
                    logger.info("Topic: " + this.topic + " - Seeking partition " + partitionInfo.getPartition() + " to offset " + partitionInfo.getLastOffset());

                    consumer.seek(new TopicPartition(this.topic, partitionInfo.getPartition()), partitionInfo.getLastOffset());
                }
            } else { // If this is a rebalancing
                logger.info("Topic: " + this.topic + " - Cleaning for rebalance");

                try {
                    timeBasedPartitioner.clearPartitions();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            logger.info("Topic: " + this.topic + " - Revoking partitions");
        }
    }
}
