package com.grupozap.dumping_machine.writers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class AvroParquetRecordWriter {
    private final long createdAt;
    private final String filename;
    private final String path;
    private final ParquetWriter writer;

    public AvroParquetRecordWriter(Schema schema, String path, String filename, int blockSize, int pageSize) throws IOException {
        this.writer = AvroParquetWriter.<GenericRecord>builder(new Path(path + filename))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(blockSize)
                .withPageSize(pageSize)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
        this.createdAt = System.currentTimeMillis();
        this.path = path;
        this.filename = filename;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void write(GenericRecord record) {
        try {
            if(record != null) {
                this.writer.write(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        try {
            this.writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getPath() {
        return path;
    }

    public String getFilePath() {
        return path + filename;
    }
}
