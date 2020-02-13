package com.grupozap.dumping_machine.partitioners;

import com.grupozap.dumping_machine.consumers.TimeBasedRecordConsumer;
import com.grupozap.dumping_machine.deserializers.RecordType;
import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.metastore.HiveClient;
import com.grupozap.dumping_machine.metastore.HiveUtil;
import com.grupozap.dumping_machine.uploaders.Uploader;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class TimeBasedPartitioner {
    private final Logger logger = LoggerFactory.getLogger(TimeBasedPartitioner.class);

    private HashMap<TimeBasedRecordConsumer, ArrayList<PartitionInfo>> writerPartitionInfos;
    private ArrayList<PartitionInfo> partitionInfos;

    private final String topic;
    private final Uploader uploader;
    private final long partitionForget;
    private final String metaStoreUris;
    private final HashMap<RecordType, String> hiveTables;
    private final String partitionPattern;

    private final long waitFor = 300000;

    public TimeBasedPartitioner(String topic, Uploader uploader, long partitionForget, String metaStoreUris, HashMap<RecordType, String> hiveTables, String partitionPattern) {
        this.topic = topic;
        this.uploader = uploader;
        this.hiveTables = hiveTables;
        this.partitionForget = partitionForget;
        this.metaStoreUris = metaStoreUris;
        this.partitionPattern = partitionPattern;
        this.writerPartitionInfos = new HashMap<>();
        this.partitionInfos = new ArrayList<>();
    }

    public void consume(AvroExtendedMessage record) throws IOException {
        if (validateDuplicateMessage(this.partitionInfos, record)) {
            this.writerPartitionInfos = this.addOrUpdateWriter(this.writerPartitionInfos, record);

            this.partitionInfos = this.addOrUpdatePartitionInfo(this.partitionInfos, record);
        }
    }

    public Map<TopicPartition, OffsetAndMetadata> commitWriters() throws Exception {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = new HashMap<>();
        ArrayList<TimeBasedRecordConsumer> closedTimeBasedRecordConsumers = this.getClosedWriters();

        for (TimeBasedRecordConsumer timeBasedRecordConsumer : closedTimeBasedRecordConsumers) {
            for (PartitionInfo partitionInfo : writerPartitionInfos.get(timeBasedRecordConsumer)) {
                // TODO: Sort writers by creation time instead of ordering the result by offsets
                if (topicPartitionOffsetAndMetadataMap.get(partitionInfo.getTopicPartition()) == null || topicPartitionOffsetAndMetadataMap.get(partitionInfo.getTopicPartition()).offset() < partitionInfo.getLastOffset()) {
                    topicPartitionOffsetAndMetadataMap.put(partitionInfo.getTopicPartition(), partitionInfo.getNextOffsetAndMetadata());
                }
            }

            this.closeWriter(timeBasedRecordConsumer);
        }

        this.writerPartitionInfos.keySet().removeAll(closedTimeBasedRecordConsumers);

        return topicPartitionOffsetAndMetadataMap;
    }

    public void clearPartitions() throws IOException {
        for (TimeBasedRecordConsumer timeBasedRecordConsumer : this.writerPartitionInfos.keySet()) {
            timeBasedRecordConsumer.close();
            timeBasedRecordConsumer.delete();
        }

        this.writerPartitionInfos = new HashMap<>();
        this.partitionInfos = new ArrayList<>();
    }

    public ArrayList<PartitionInfo> getPartitionInfos() {
        return this.partitionInfos;
    }

    private HashMap<TimeBasedRecordConsumer, ArrayList<PartitionInfo>> addOrUpdateWriter(HashMap<TimeBasedRecordConsumer, ArrayList<PartitionInfo>> localWriterPartitionInfos, AvroExtendedMessage record) throws IOException {
        TimeBasedRecordConsumer recordTimeBasedRecordConsumer = null;
        ArrayList<PartitionInfo> localPartitionInfos = new ArrayList<>();

        for (TimeBasedRecordConsumer timeBasedRecordConsumer : localWriterPartitionInfos.keySet()) {
            if (timeBasedRecordConsumer.getMinTimestamp() <= record.getTimestamp() && record.getTimestamp() <= timeBasedRecordConsumer.getMaxTimestamp()) {
                recordTimeBasedRecordConsumer = timeBasedRecordConsumer;
            }
        }

        if (recordTimeBasedRecordConsumer == null) {
            recordTimeBasedRecordConsumer = new TimeBasedRecordConsumer(this.topic, record.getTimestamp(), this.partitionPattern);
        } else {
            localPartitionInfos = localWriterPartitionInfos.get(recordTimeBasedRecordConsumer);
        }

        localPartitionInfos = this.addOrUpdatePartitionInfo(localPartitionInfos, record);

        recordTimeBasedRecordConsumer.write(record);

        localWriterPartitionInfos.put(recordTimeBasedRecordConsumer, localPartitionInfos);

        logger.trace("Topic: " + this.topic + " - Consuming message (Partition: " + record.getPartition() + ", Offset: " + record.getOffset() + ")");

        return localWriterPartitionInfos;
    }

    private ArrayList<PartitionInfo> addOrUpdatePartitionInfo(ArrayList<PartitionInfo> localPartitionInfos, AvroExtendedMessage record) {
        Integer partitionIndex = null;
        PartitionInfo partitionInfo;

        for (PartitionInfo partition : localPartitionInfos) {
            if (partition.getPartition() == record.getPartition()) {
                partitionIndex = localPartitionInfos.indexOf(partition);
            }
        }

        if (partitionIndex == null) {
            logger.info("Topic: " + this.topic + " - Appending partition " + record.getPartition());
            partitionInfo = new PartitionInfo(this.topic, record.getPartition(), record.getOffset());
            localPartitionInfos.add(partitionInfo);
        } else {
            partitionInfo = localPartitionInfos.get(partitionIndex);

            partitionInfo.setLastOffset(record.getOffset());

            localPartitionInfos.set(partitionIndex, partitionInfo);
        }

        return localPartitionInfos;
    }

    private ArrayList<TimeBasedRecordConsumer> getClosedWriters() {
        ArrayList<TimeBasedRecordConsumer> removedTimeBasedRecordConsumers = new ArrayList<>();

        for (Map.Entry<TimeBasedRecordConsumer, ArrayList<PartitionInfo>> entry : this.writerPartitionInfos.entrySet()) {
            if ((arePartitionsClosed(entry.getValue()) && entry.getKey().getUpdateTimestamp() + this.waitFor < System.currentTimeMillis()) || entry.getKey().getUpdateTimestamp() + this.partitionForget < System.currentTimeMillis()) {
                removedTimeBasedRecordConsumers.add(entry.getKey());
            }
        }

        return removedTimeBasedRecordConsumers;
    }

    private boolean arePartitionsClosed(ArrayList<PartitionInfo> partitions) {
        for (PartitionInfo partitionInfo : partitions) {
            for (PartitionInfo partitionerPartitionInfo : this.partitionInfos) {
                if (partitionerPartitionInfo.getPartition() == partitionInfo.getPartition() && partitionerPartitionInfo.getLastOffset() <= partitionInfo.getLastOffset()) {
                    return false;
                }
            }
        }

        return true;
    }

    private void closeWriter(TimeBasedRecordConsumer timeBasedRecordConsumer) throws Exception {
        timeBasedRecordConsumer.close();

        for (Map.Entry<RecordType, HashMap<String, String>> entry : timeBasedRecordConsumer.getFilePaths().entrySet()) {
            for (Map.Entry<String, String> path : entry.getValue().entrySet()) {

                String hiveTable = hiveTables.get(entry.getKey());

                Schema schema = timeBasedRecordConsumer.getSchema(path.getKey());

                logger.info("Topic: " + this.topic + " - Uploading hourlyBasedRecordConsumer for " + this.topic + " path " + path.getValue());

                this.uploader.upload(path.getValue(), path.getKey());

                if (hiveTable != null) {
                    HiveClient hiveClient = new HiveClient(this.metaStoreUris);
                    HiveUtil.updateHive(hiveClient, hiveTable, schema, path.getValue(), this.uploader.getServerPath());
                }
            }
        }

        timeBasedRecordConsumer.delete();
    }

    private boolean validateDuplicateMessage(ArrayList<PartitionInfo> localPartitionInfos, AvroExtendedMessage record){
        Integer partitionIndex = null;
        PartitionInfo partitionInfo;

        for (PartitionInfo partition : localPartitionInfos) {
            if (partition.getPartition() == record.getPartition()) {
                partitionIndex = localPartitionInfos.indexOf(partition);
            }
        }

        if (partitionIndex == null) {
            return true;
        } else {
            partitionInfo = localPartitionInfos.get(partitionIndex);

            if(partitionInfo.getLastOffset() >= record.getOffset()) {
                logger.warn("Topic: " + this.topic + " - Message from partition " + record.getPartition() +  " with offset " + record.getOffset() + " less than the last offset " + partitionInfo.getLastOffset());
                return false;
            }
        }
        return true;
    }
}
