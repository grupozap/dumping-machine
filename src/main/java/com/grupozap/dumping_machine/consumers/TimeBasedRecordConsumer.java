package com.grupozap.dumping_machine.consumers;

import com.grupozap.dumping_machine.deserializers.RecordType;
import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.writers.RecordWriter;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class TimeBasedRecordConsumer implements RecordConsumer {
    private final Logger logger = LoggerFactory.getLogger(TimeBasedRecordConsumer.class);

    private HashMap<Schema, RecordWriter> recordWriters;
    private HashMap<String, Schema> pathSchemas;
    private RecordWriter tombstoneWriter;
    private RecordWriter errorWriter;

    private final String localPath = "./tmp/parquet/";

    private final String topic;
    private final long firstTimestamp;
    private final String partitionPattern;
    private long updateTimestamp;

    public TimeBasedRecordConsumer(String topic, long firstTimestamp, String partitionPattern) {
        this.topic = topic;
        this.firstTimestamp = firstTimestamp;
        this.updateTimestamp = System.currentTimeMillis();
        this.partitionPattern = partitionPattern;
        this.recordWriters = new HashMap<>();
        this.pathSchemas = new HashMap<>();

    }

    @Override
    public void write(AvroExtendedMessage record) throws IOException {
        RecordType recordType = record.getType();
        RecordWriter recordWriter;

        if (recordType == RecordType.TOMBSTONE) {
            if (tombstoneWriter == null) {
                tombstoneWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
                pathSchemas.put(tombstoneWriter.getPath() + tombstoneWriter.getFilename(), record.getSchema());
            }

            recordWriter = tombstoneWriter;
        } else if (recordType == RecordType.ERROR) {
            if (errorWriter == null) {
                errorWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
                pathSchemas.put(errorWriter.getPath() + errorWriter.getFilename(), record.getSchema());
            }

            recordWriter = errorWriter;
        } else {
            // TODO: We must validate the schema before adding a new file
            recordWriter = recordWriters.get(record.getSchema());

            if (recordWriter == null) {
                recordWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
                pathSchemas.put(recordWriter.getPath() + recordWriter.getFilename(), record.getSchema());
            }

            recordWriters.put(record.getSchema(), recordWriter);
        }

        recordWriter.write(record.getRecord());

        this.updateTimestamp = System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        for (RecordWriter recordWriter : this.recordWriters.values()) {
            recordWriter.close();
        }

        if (tombstoneWriter != null) {
            tombstoneWriter.close();
        }

        if (errorWriter != null) {
            errorWriter.close();
        }

    }

    @Override
    public void delete() throws IOException {
        for (RecordWriter recordWriter : this.recordWriters.values()) {
            deleteWriter(recordWriter);
        }

        this.recordWriters = new HashMap<>();

        if (tombstoneWriter != null) {
            deleteWriter(tombstoneWriter);
            this.tombstoneWriter = null;
        }

        if (errorWriter != null) {
            deleteWriter(errorWriter);
            this.errorWriter = null;
        }

        this.pathSchemas = new HashMap<>();
    }

    private void deleteWriter(RecordWriter writer) throws IOException {
        writer.close();
        new File(writer.getPath() + writer.getFilename()).delete();
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public String getPartitionPath() {
        SimpleDateFormat sdf = new SimpleDateFormat(this.partitionPattern);
        Date date = new Date(this.getFirstTimestamp());

        return sdf.format(date);
    }

    public long getMaxTimestamp() {
        Calendar cal = Calendar.getInstance();

        cal.setTimeInMillis(this.firstTimestamp);
        cal.set(Calendar.MINUTE, 59);
        cal.set(Calendar.SECOND, 59);
        cal.set(Calendar.MILLISECOND, 999);

        return cal.getTimeInMillis();
    }

    public long getMinTimestamp() {
        Calendar cal = Calendar.getInstance();

        cal.setTimeInMillis(this.firstTimestamp);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return cal.getTimeInMillis();
    }

    private RecordWriter createFile(Schema schema, RecordType recordType, int partition, long offset) throws IOException {
        String localPartitionPath = this.localPath + this.topic + "/" + recordType.toString().toLowerCase() + "/" + this.getPartitionPath();
        String fileName = partition + "_" + offset + ".parquet";

        logger.info("Topic: " + this.topic + " - Opening file " + localPartitionPath + fileName + " for partition " + partition + " and type " + recordType);

        int pageSize = 64 * 1024;
        int blockSize = 256 * 1024 * 1024;

        RecordWriter recordWriter = new RecordWriter(
                schema,
                localPartitionPath,
                fileName,
                blockSize,
                pageSize
        );

        return recordWriter;
    }

    public HashMap<RecordType, HashMap<String, String>> getFilePaths() {
        HashMap<RecordType, HashMap<String, String>> recordTypePaths = new HashMap<>();

        if (!recordWriters.isEmpty()) {
            HashMap<String, String> filePaths = new HashMap<>();

            for (RecordWriter recordWriter : recordWriters.values()) {
                filePaths.put(
                        recordWriter.getPath() + recordWriter.getFilename(),
                        recordWriter.getPath().replaceFirst(this.localPath, "") + recordWriter.getFilename()
                );
            }

            recordTypePaths.put(RecordType.RECORD, filePaths);
        }

        if (tombstoneWriter != null) {
            HashMap<String, String> tombstoneWriterFilePath = new HashMap<>();

            tombstoneWriterFilePath.put(
                    tombstoneWriter.getPath() + tombstoneWriter.getFilename(),
                    tombstoneWriter.getPath().replaceFirst(this.localPath, "") + tombstoneWriter.getFilename()
            );

            recordTypePaths.put(RecordType.TOMBSTONE, tombstoneWriterFilePath);
        }

        if (errorWriter != null) {
            HashMap<String, String> errorWriterFilePath = new HashMap<>();

            errorWriterFilePath.put(
                    errorWriter.getPath() + errorWriter.getFilename(),
                    errorWriter.getPath().replaceFirst(this.localPath, "") + errorWriter.getFilename()
            );

            recordTypePaths.put(RecordType.ERROR, errorWriterFilePath);
        }

        return recordTypePaths;
    }

    public Schema getSchema(String path) {
        return this.pathSchemas.get(path);
    }
}
