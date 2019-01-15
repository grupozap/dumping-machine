package com.grupozap.dumping_machine.partitioners;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.uploaders.Uploader;
import com.grupozap.dumping_machine.writers.Writer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.text.SimpleDateFormat;
import java.util.*;

public class HourlyBasedPartitioner {
    private HashMap<Writer, ArrayList<PartitionInfo>> partitions;

    private final String topic;
    private final Uploader uploader;
    private final long partitionForget;

    public HourlyBasedPartitioner(String topic, Uploader uploader, long partitionForget) {
        this.topic = topic;
        this.uploader = uploader;
        this.partitionForget = partitionForget;
        this.partitions = new HashMap<>();
    }

    public void consume(AvroExtendedMessage record) {
        this.partitions = this.addOrUpdateWriter(this.partitions, record);
    }

    private HashMap<Writer, ArrayList<PartitionInfo>> addOrUpdateWriter(HashMap<Writer, ArrayList<PartitionInfo>> partitions, AvroExtendedMessage record) {
        Writer recordWriter = null;
        ArrayList<PartitionInfo> partitionInfo = new ArrayList<>();
        long timestamp = record.getTimestamp();

        for(Map.Entry<Writer, ArrayList<PartitionInfo>> pair : partitions.entrySet()) {
            if(this.getMinTimestamp(pair.getKey().getFirstTimestamp()) < timestamp && timestamp < this.getMaxTimestamp(pair.getKey().getFirstTimestamp())) {
                recordWriter = pair.getKey();
            }
        }

        if(recordWriter == null) {
            recordWriter = new Writer(record.getPartition(), record.getOffset(), timestamp, System.currentTimeMillis());
        } else {
            partitionInfo = partitions.get(recordWriter);
        }

        partitionInfo = this.addOrUpdatePartitionInfo(partitionInfo, record);
        partitions.put(recordWriter, partitionInfo);

        recordWriter.write(record);

        return partitions;
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

    private ArrayList<PartitionInfo> addOrUpdatePartitionInfo(ArrayList<PartitionInfo> partitionInfos, AvroExtendedMessage record) {
        Integer partitionIndex = null;

        for(PartitionInfo partitionInfo : partitionInfos) {
            if(partitionInfo.getPartition() == record.getPartition()) {
                partitionIndex = partitionInfos.indexOf(partitionInfo);
            }
        }

        if(partitionIndex == null) {
            PartitionInfo partitionInfo = new PartitionInfo(record.getPartition(), record.getOffset());

            partitionInfos.add(partitionInfo);
        } else {
            PartitionInfo partitionInfo = partitionInfos.get(partitionIndex);

            partitionInfo.setPartition(record.getPartition());
            partitionInfo.setOffset(record.getOffset());

            partitionInfos.set(partitionIndex, partitionInfo);
        }

        return partitionInfos;
    }

    public Map<TopicPartition, OffsetAndMetadata> getClosedPartitions() {
        Map<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataMap = new HashMap<>();
        ArrayList<Writer> removedWriters = new ArrayList<>();

        for(Map.Entry<Writer, ArrayList<PartitionInfo>> entry : this.partitions.entrySet()) {
            if (System.currentTimeMillis() > entry.getKey().getLastTimestamp() + this.partitionForget) {
                for(PartitionInfo partitionInfo : entry.getValue()) {
                    TopicPartition topicPartition = new TopicPartition(this.topic, partitionInfo.getPartition());
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(partitionInfo.getOffset());

                    // TODO: Sort writers by creation time instead of ordering the result by offsets
                    if(topicPartitionOffsetAndMetadataMap.get(topicPartition) == null || ( topicPartitionOffsetAndMetadataMap.get(topicPartition) != null && topicPartitionOffsetAndMetadataMap.get(topicPartition).offset() < offsetAndMetadata.offset() )) {
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

    private void closeWriter(Writer writer) {
        writer.close();
        this.uploader.upload(this.topic + "/" + this.getPartitionPath(writer) + "/" + writer.getFilename(), writer.getLocalPath() + writer.getFilename());
        writer.delete();
    }

    private String getPartitionPath(Writer writer) {
        SimpleDateFormat dayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hourDateFormat = new SimpleDateFormat("HH");
        Date date = new Date(writer.getFirstTimestamp());

        return "dt=" + dayDateFormat.format(date) + "/hr=" + hourDateFormat.format(date);
    }
}
