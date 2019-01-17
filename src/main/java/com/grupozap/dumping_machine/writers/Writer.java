package com.grupozap.dumping_machine.writers;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import org.apache.avro.Schema;

import java.io.File;
import java.io.IOException;

public class Writer {
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private final String localPath = "./tmp/parquet/";

    private final String topic;
    private final int partition;
    private final long offset;
    private final long firstTimestamp;
    private final long creationTimestamp;

    public Writer(String topic, int partition, long offset, long firstTimestamp, long creationTimestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.firstTimestamp = firstTimestamp;
        this.creationTimestamp = creationTimestamp;
    }

    public void write(AvroExtendedMessage record) {
        if(avroParquetRecordWriter == null) {
            avroParquetRecordWriter = createFile(record.getSchema());
        }

        avroParquetRecordWriter.write(record.getRecord());
    }

    public void close() {
        this.avroParquetRecordWriter.close();
    }

    public void delete() { new File(this.getLocalPath() + getFilename()).delete(); }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    private AvroParquetRecordWriter createFile(Schema schema) {
        try {
            int pageSize = 64 * 1024;
            int blockSize = 256 * 1024 * 1024;
            avroParquetRecordWriter = new AvroParquetRecordWriter(schema, this.getLocalPath(), getFilename(), blockSize, pageSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return avroParquetRecordWriter;
    }

    public String getLocalPath() {
        return this.localPath + this.topic + "/";
    }

    public String getFilename() {
        return offset + "_" + partition + ".parquet";
    }
}
