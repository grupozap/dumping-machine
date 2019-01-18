package com.grupozap.dumping_machine.partitioners;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.uploaders.Uploader;
import com.grupozap.dumping_machine.writers.Writer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

public class HourlyBasedPartitioner {
    private final Logger logger = LoggerFactory.getLogger(HourlyBasedPartitioner.class);

    private HashMap<Writer, HashMap<Integer, PartitionInfo>> partitions;
    private HashMap<Integer, PartitionInfo> partitionInfos;

    private final String topic;
    private final Uploader uploader;
    private final long partitionForget;

    public HourlyBasedPartitioner(String topic, Uploader uploader, long partitionForget) {
        this.topic = topic;
        this.uploader = uploader;
        this.partitionForget = partitionForget;
        this.partitions = new HashMap<>();
        this.partitionInfos = new HashMap<>();
    }

    public void consume(AvroExtendedMessage record) {
        this.partitions = this.addOrUpdateWriter(this.partitions, record);

        this.partitionInfos = this.addOrUpdatePartitionInfo(this.partitionInfos, record);
    }

    private HashMap<Writer, HashMap<Integer, PartitionInfo>> addOrUpdateWriter(HashMap<Writer, HashMap<Integer, PartitionInfo>> partitions, AvroExtendedMessage record) {
        Writer recordWriter = null;
        HashMap<Integer, PartitionInfo> partitionInfo = new HashMap<>();
        long timestamp = record.getTimestamp();

        for(Writer writer : partitions.keySet()) {
            if(this.getMinTimestamp(writer.getFirstTimestamp()) <= timestamp && timestamp <= this.getMaxTimestamp(writer.getFirstTimestamp())) {
                recordWriter = writer;
            }
        }

        if(recordWriter == null) {
            recordWriter = new Writer(this.topic, record.getPartition(), record.getOffset(), timestamp, System.currentTimeMillis());
            logger.info("Topic: " + this.topic + " - Opening writer for partition " + record.getPartition());
        } else {
            partitionInfo = partitions.get(recordWriter);
        }

        partitionInfo = this.addOrUpdatePartitionInfo(partitionInfo, record);
        partitions.put(recordWriter, partitionInfo);

        recordWriter.write(record);
        logger.trace("Topic: " + this.topic + " - Consuming message (Partition: " + record.getPartition() + ", Offset: " + record.getOffset() + ")");

        return partitions;
    }

    private HashMap<Integer, PartitionInfo> addOrUpdatePartitionInfo(HashMap<Integer, PartitionInfo> partitionInfos, AvroExtendedMessage record) {
        PartitionInfo partitionInfo = partitionInfos.get(record.getPartition());

        if(partitionInfo == null) {
            logger.info("Topic: " + this.topic + " - Appending partition " + record.getPartition());
            partitionInfo = new PartitionInfo(record.getPartition(), record.getOffset());
        } else {
            partitionInfo.setPartition(record.getPartition());
            partitionInfo.setOffset(record.getOffset());
        }

        partitionInfos.put(record.getPartition(), partitionInfo);

        return partitionInfos;
    }

    public Map<TopicPartition, OffsetAndMetadata> getClosedPartitions() {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = new HashMap<>();
        ArrayList<Writer> removedWriters = new ArrayList<>();

        for(Map.Entry<Writer, HashMap<Integer, PartitionInfo>> entry : this.partitions.entrySet()) {
            if(entry.getKey().getUpdateTimestamp() + this.partitionForget < System.currentTimeMillis() && isPartitionClosed(entry.getValue())) {
                for(PartitionInfo partitionInfo : entry.getValue().values()) {
                    TopicPartition topicPartition = new TopicPartition(this.topic, partitionInfo.getPartition());
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(partitionInfo.getOffset());

                    // TODO: Sort writers by creation time instead of ordering the result by offsets
                    if (topicPartitionOffsetAndMetadataMap.get(topicPartition) == null || topicPartitionOffsetAndMetadataMap.get(topicPartition).offset() < offsetAndMetadata.offset()) {
                        topicPartitionOffsetAndMetadataMap.put(topicPartition, offsetAndMetadata);
                    }
                }

                removedWriters.add(entry.getKey());
            }
        }

        for(Writer writer : removedWriters) {
            this.closeWriter(writer);
            this.partitions.remove(writer);
        }

        return topicPartitionOffsetAndMetadataMap;
    }

    private long getMaxTimestamp(long timestamp) {
        Calendar cal = Calendar.getInstance();

        cal.setTimeInMillis(timestamp);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTimeInMillis();
    }

    private long getMinTimestamp(long timestamp) {
        Calendar cal = Calendar.getInstance();

        cal.setTimeInMillis(timestamp);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    private boolean isPartitionClosed(HashMap<Integer, PartitionInfo> writerPartitions) {
        int partitionsClosed = 0;

        for(PartitionInfo partitionInfo : writerPartitions.values()) {
            if (partitionInfo.getOffset() < this.partitionInfos.get(partitionInfo.getPartition()).getOffset()) {
                partitionsClosed++;
            }
        }

        return writerPartitions.size() == partitionsClosed;
    }

    private void closeWriter(Writer writer) {
        writer.close();

        logger.info("Topic: " + this.topic + " - Uploading writer for " + this.topic + " path " + writer.getLocalPath() + writer.getFilename());

        this.uploader.upload(this.topic + "/" + this.getPartitionPath(writer) + "/" + writer.getFilename(), writer.getLocalPath() + writer.getFilename());
        writer.delete();
    }

    private String getPartitionPath(Writer writer) {
        SimpleDateFormat dayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hourDateFormat = new SimpleDateFormat("HH");
        Date date = new Date(writer.getFirstTimestamp());

        return "dt=" + dayDateFormat.format(date) + "/hr=" + hourDateFormat.format(date);
    }

    public void clearPartitions() {
        for(Writer writer : this.partitions.keySet()) {
            writer.close();
            writer.delete();
        }

        this.partitions = new HashMap<>();
        this.partitionInfos = new HashMap<>();
    }

    public HashMap<Integer, PartitionInfo> getPartitionInfos() {
        return this.partitionInfos;
    }
}
