package com.grupozap.dumping_machine.consumers;

import com.grupozap.dumping_machine.deserializers.RecordType;
import com.grupozap.dumping_machine.formaters.AvroExtendedMessage;
import com.grupozap.dumping_machine.writers.RecordWriter;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

public class HourlyBasedRecordConsumer implements RecordConsumer {
    private final Logger logger = LoggerFactory.getLogger(HourlyBasedRecordConsumer.class);

    private HashMap<RecordType, RecordWriter> recordWriters;

    private final String localPath = "./tmp/parquet/";

    private final String topic;
    private final int partition;
    private final long offset;
    private final long firstTimestamp;
    private long updateTimestamp;

    public HourlyBasedRecordConsumer(String topic, int partition, long offset, long firstTimestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.firstTimestamp = firstTimestamp;
        this.updateTimestamp = System.currentTimeMillis();
        this.recordWriters = new HashMap<>();
    }

    @Override
    public void write(AvroExtendedMessage record) {
        RecordWriter recordWriter = recordWriters.get(record.getType());

        if(recordWriter == null) {
            logger.info("Topic: " + this.topic + " - Opening file for partition " + record.getPartition() + " and type " + record.getType());

            recordWriter = createFile(record.getSchema(), record.getType());
        }

        recordWriter.write(record.getRecord());

        recordWriters.put(record.getType(), recordWriter);

        this.updateTimestamp = System.currentTimeMillis();
    }

    @Override
    public void close() {
        for(RecordWriter recordWriter : this.recordWriters.values()) {
            recordWriter.close();
        }
    }

    @Override
    public void delete() {
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

    private RecordWriter createFile(Schema schema, RecordType recordType) {
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

    public HashMap<String, String> getFilePaths() {
        HashMap<String, String> filePaths = new HashMap<>();

        for(RecordWriter recordWriter : recordWriters.values()) {
            filePaths.put(
                    recordWriter.getPath() + recordWriter.getFilename(),
                    recordWriter.getPath().replaceFirst(this.localPath, "") + recordWriter.getFilename()
            );
        }

        return filePaths;
    }
}
