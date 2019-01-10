package com.grupozap.dumping_machine.partitioners;

import com.grupozap.dumping_machine.uploaders.S3Uploader;
import com.grupozap.dumping_machine.writers.AvroParquetRecordWriter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class Partition {
    private AvroParquetRecordWriter avroParquetRecordWriter;
    private final int blockSize = 256 * 1024 * 1024;
    private final int pageSize = 64 * 1024;
    private final long uploadMillis = 60000;

    public void write(ConsumerRecord<String, GenericRecord> record) {
        if(avroParquetRecordWriter == null) {
            avroParquetRecordWriter = createFile(record.value().getSchema());
        }

        if(record != null) {
            avroParquetRecordWriter.write(record.value());
        }
    }

    public boolean closeIfOld() {
        boolean close;

        close = ((System.currentTimeMillis() - avroParquetRecordWriter.getCreatedAt()) > uploadMillis);

        if(close == true) {
            this.closeFile();
            this.uploadFile();

            avroParquetRecordWriter = null;
        }

        return close;
    }

    private AvroParquetRecordWriter createFile(Schema schema) {
        try {
            avroParquetRecordWriter = new AvroParquetRecordWriter(schema, this.localPath, generateFilename(), blockSize, pageSize);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return avroParquetRecordWriter;
    }

    private void closeFile() {
        if (avroParquetRecordWriter != null) {
            avroParquetRecordWriter.close();
        }
    }

    private void uploadFile() {
        if (avroParquetRecordWriter != null) {
            new S3Uploader().upload(this.remotePath + generateFilename(), avroParquetRecordWriter.getFilePath());
        }
    }

    private String generateFilename() {
        List<String> offsets = offsets().stream().map(Object::toString).collect(Collectors.toList());
        List<String> partitions = partitions().stream().map(Object::toString).collect(Collectors.toList());

        String offsetAndPartitions = String.format("%s%s", topic, String.join("", offsets), String.join("", partitions));

        return DigestUtils.md5Hex(offsetAndPartitions) + ".parquet";
    }
}
