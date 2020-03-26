package com.grupozap.dumping_machine.metastore;

import com.amazonaws.services.glue.model.Column;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.avro.Schema;

public class AvroToAWSGlue implements AvroToMetastore<Column> {

    public List<Column> getPartitions(String partitionPattern) {
        List partitions = new ArrayList<Column>();
        Pattern p = Pattern.compile("\'([^\']*)\'");
        Matcher m = p.matcher(partitionPattern);

        while (m.find()) {
            String partitionName = m.group(1).replace("/", "").replace("=", "");
            partitions.add(new Column()
                    .withName(partitionName)
                    .withType(TypeInfoFactory.dateTypeInfo.getTypeName())
                    .withComment("PARTITIONED BY " + partitionName));
        }

        return partitions;
    }

    public List<Column> generateSchema(Schema schema) throws Exception {
        Class<?> schemaToTypeInfo = Class.forName("org.apache.hadoop.hive.serde2.avro.SchemaToTypeInfo");
        Method generateTypeInfo = schemaToTypeInfo.getDeclaredMethod("generateTypeInfo", Schema.class, Set.class);
        generateTypeInfo.setAccessible(true);

        List columns = new ArrayList<Column>();

        for (Schema.Field field : schema.getFields()) {
            TypeInfo typeInfo = (TypeInfo) generateTypeInfo.invoke(field.schema(), field.schema(), null);
            columns.add(new Column()
                    .withName(field.name().toLowerCase())
                    .withType(typeInfo.getTypeName())
                    .withComment(field.doc()));
        }

        return columns;
    }

}
