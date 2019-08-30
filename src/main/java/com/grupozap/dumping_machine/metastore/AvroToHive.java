package com.grupozap.dumping_machine.metastore;

import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.avro.Schema;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;

abstract class AvroToHive {

    static List<FieldSchema> getPartitions() {
        List<FieldSchema> partitions = new ArrayList<>();

        FieldSchema partitionDt = new FieldSchema("dt", TypeInfoFactory.dateTypeInfo.getTypeName(), "PARTITIONED BY dt");
        FieldSchema partitionHr = new FieldSchema("hr", TypeInfoFactory.intTypeInfo.getTypeName(), "PARTITIONED BY hr");

        partitions.add(partitionDt);
        partitions.add(partitionHr);

        return partitions;
    }

    static List<FieldSchema> generateSchema(Schema schema) throws Exception {

        // TODO: reflection was used because the SchemaToTypeInfo class is private
        Class<?> schemaToTypeInfo = Class.forName("org.apache.hadoop.hive.serde2.avro.SchemaToTypeInfo");
        Method generateTypeInfo = schemaToTypeInfo.getDeclaredMethod("generateTypeInfo", Schema.class, Set.class);
        generateTypeInfo.setAccessible(true);

        List<FieldSchema> columns = new ArrayList<>();

        for (Schema.Field field : schema.getFields()) {
            TypeInfo typeInfo = (TypeInfo) generateTypeInfo.invoke(field.schema(), field.schema(), null);
            columns.add(new FieldSchema(field.name().toLowerCase(), typeInfo.getTypeName(), field.doc()));
        }
        return columns;
    }
}
