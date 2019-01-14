package com.grupozap.dumping_machine.writers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Writer {
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private final int blockSize = 256 * 1024 * 1024;
    private final int pageSize = 64 * 1024;
    private final String localPath = "./tmp/parquet/";

    private int partition;
    private long offset;
    private final long firstTimestamp;
    private long lastTimestamp;

    public Writer(int partition, long offset, long firstTimestamp, long lastTimestamp) {
        this.partition = partition;
        this.offset = offset;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
    }

    public void write(ConsumerRecord<String, GenericRecord> record) {
        if(avroParquetRecordWriter == null) {
            avroParquetRecordWriter = createFile(record.value().getSchema());
        }

        avroParquetRecordWriter.write(record.value());
    }

    public void close() {
        this.avroParquetRecordWriter.close();
    }

    public void delete() { new File(this.getLocalPath() + getFilename()).delete(); }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    private AvroParquetRecordWriter createFile(Schema schema) {
        try {
            avroParquetRecordWriter = new AvroParquetRecordWriter(schema, this.localPath, getFilename(), blockSize, pageSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return avroParquetRecordWriter;
    }

    public String getLocalPath() {
        return this.localPath;
    }

    public String getFilename() {
        return offset + "_" + partition + ".parquet";
    }
}
