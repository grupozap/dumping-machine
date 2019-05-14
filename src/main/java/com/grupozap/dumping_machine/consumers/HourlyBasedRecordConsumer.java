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

public class HourlyBasedRecordConsumer implements RecordConsumer {
    private final Logger logger = LoggerFactory.getLogger(HourlyBasedRecordConsumer.class);

    private HashMap<Schema, RecordWriter> recordWriters;
    private RecordWriter tombstoneWriter;
    private RecordWriter errorWriter;

    private final String localPath = "./tmp/parquet/";

    private final String topic;
    private final long firstTimestamp;
    private long updateTimestamp;

    public HourlyBasedRecordConsumer(String topic, long firstTimestamp) {
        this.topic = topic;
        this.firstTimestamp = firstTimestamp;
        this.updateTimestamp = System.currentTimeMillis();
        this.recordWriters = new HashMap<>();
    }

    @Override
    public void write(AvroExtendedMessage record) throws IOException {
        RecordType recordType = record.getType();
        RecordWriter recordWriter;

        if(recordType == RecordType.TOMBSTONE) {
            if(tombstoneWriter == null) {
                tombstoneWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
            }

            recordWriter = tombstoneWriter;
        } else if (recordType == RecordType.ERROR) {
            if(errorWriter == null) {
                errorWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
            }

            recordWriter = errorWriter;
        } else {
            // TODO: We must validate the schema before adding a new file
            recordWriter = recordWriters.get(record.getSchema());

            if(recordWriter == null) {
                recordWriter = createFile(record.getSchema(), record.getType(), record.getPartition(), record.getOffset());
            }

            recordWriters.put(record.getSchema(), recordWriter);
        }

        recordWriter.write(record.getRecord());

        this.updateTimestamp = System.currentTimeMillis();
    }

    @Override
    public void close() throws IOException {
        for(RecordWriter recordWriter : this.recordWriters.values()) {
            recordWriter.close();
        }
    }

    @Override
    public void delete() throws IOException {
        for(RecordWriter recordWriter : this.recordWriters.values()) {
            recordWriter.close();
            new File(recordWriter.getPath() + recordWriter.getFilename()).delete();
        }

        this.recordWriters = new HashMap<>();
    }

    public long getFirstTimestamp() {
        return firstTimestamp;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public String getPartitionPath() {
        SimpleDateFormat dayDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hourDateFormat = new SimpleDateFormat("HH");
        Date date = new Date(this.getFirstTimestamp());

        return "dt=" + dayDateFormat.format(date) + "/hr=" + hourDateFormat.format(date) + "/";
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
        logger.info("Topic: " + this.topic + " - Opening file for partition " + partition + " and type " + recordType);

        int pageSize = 64 * 1024;
        int blockSize = 256 * 1024 * 1024;

        RecordWriter recordWriter = new RecordWriter(
                schema,
                this.localPath + this.topic + "/" + recordType.toString().toLowerCase() + "/" + this.getPartitionPath(),
                partition + "_" + offset + ".parquet",
                blockSize,
                pageSize
        );

        return recordWriter;
    }

    public HashMap<RecordType, HashMap<String, String>> getFilePaths() {
        HashMap<RecordType, HashMap<String, String>> recordTypePaths = new HashMap<>();

        if(!recordWriters.isEmpty()) {
            HashMap<String, String> filePaths = new HashMap<>();

            for(RecordWriter recordWriter : recordWriters.values()) {
                filePaths.put(
                        recordWriter.getPath() + recordWriter.getFilename(),
                        recordWriter.getPath().replaceFirst(this.localPath, "") + recordWriter.getFilename()
                );
            }

            recordTypePaths.put(RecordType.RECORD, filePaths);
        }

        if(tombstoneWriter != null) {
            HashMap<String, String> tombstoneWriterFilePath = new HashMap<>();

            tombstoneWriterFilePath.put(
                    tombstoneWriter.getPath() + tombstoneWriter.getFilename(),
                    tombstoneWriter.getPath().replaceFirst(this.localPath, "") + tombstoneWriter.getFilename()
            );

            recordTypePaths.put(RecordType.TOMBSTONE, tombstoneWriterFilePath);
        }

        if(errorWriter != null) {
            HashMap<String, String> errorWriterFilePath = new HashMap<>();

            errorWriterFilePath.put(
                    errorWriter.getPath() + errorWriter.getFilename(),
                    errorWriter.getPath().replaceFirst(this.localPath, "") + errorWriter.getFilename()
            );

            recordTypePaths.put(RecordType.ERROR, errorWriterFilePath);
        }

        return recordTypePaths;
    }
}
