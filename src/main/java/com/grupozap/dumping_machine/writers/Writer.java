package com.grupozap.dumping_machine.writers;

import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;

public class Writer {
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private final String localPath = "./tmp/parquet/";

    private final int partition;
    private final long offset;
    private final long firstTimestamp;
    private final long lastTimestamp;

    public Writer(int partition, long offset, long firstTimestamp, long lastTimestamp) {
        this.partition = partition;
        this.offset = offset;
        this.firstTimestamp = firstTimestamp;
        this.lastTimestamp = lastTimestamp;
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

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    private AvroParquetRecordWriter createFile(Schema schema) {
        try {
            int pageSize = 64 * 1024;
            int blockSize = 256 * 1024 * 1024;
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
