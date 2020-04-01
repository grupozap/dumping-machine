package com.grupozap.dumping_machine.metastore;

import com.amazonaws.services.glue.model.Column;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;

public interface AvroToMetastore<T> {

    List<T> getPartitions(String partitionPattern);

    List<T> generateSchema(Schema schema) throws Exception;
}
