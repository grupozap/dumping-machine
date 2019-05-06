package com.grupozap.dumping_machine.writers;

import java.nio.file.Files;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class RecordWriter {
    private ParquetWriter writer;
    private final String path;
    private final String filename;

    public RecordWriter(Schema schema, String path, String filename, int blockSize, int pageSize) throws IOException {
        this.path = path;
        this.filename = filename;

        if(fileExists(path + filename))
            throw new IOException("File already exists: " + path + filename);

        this.writer = AvroParquetWriter.<GenericRecord>builder(new Path(path + filename))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(blockSize)
                .withPageSize(pageSize)
                .withDictionaryEncoding(true)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
    }

    public void write(GenericRecord record) throws IOException {
        this.writer.write(record);
    }

    public void close() throws IOException {
        this.writer.close();
    }

    public String getPath() {
        return this.path;
    }

    public String getFilename() {
        return this.filename;
    }

    private boolean fileExists(String filePath) {
        return Files.exists(java.nio.file.Paths.get(filePath));
    }
}
