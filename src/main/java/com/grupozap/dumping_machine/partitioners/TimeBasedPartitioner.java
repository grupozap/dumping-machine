package com.grupozap.dumping_machine.partitioners;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class TimeBasedPartitioner {
    private final String topic;
    private Map<String, Partition> partitions;

    public TimeBasedPartitioner(String topic) {
        this.topic = topic;
        this.partitions = new HashMap<>();
    }

    public Partition getPartition(ConsumerRecord<String, GenericRecord> record) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date(record.timestamp());
        String partitionKey = simpleDateFormat.format(date);
        Partition partition = partitions.get(partitionKey);

        if(partition == null) {
            partition = partitions.put(partitionKey, new Partition());
        }

        return partition;
    }

    public void write(ConsumerRecord<String, GenericRecord> record) {
        Partition partition = getPartition(record);

        partition.write(record);
    }

    public boolean closeOldPartitions() {
        boolean close = false;

        for(Map.Entry<String, Partition> partition : partitions.entrySet()) {
            close = close || partition.getValue().closeIfOld();
        }

        return close;
    }
}
