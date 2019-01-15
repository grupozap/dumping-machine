package com.grupozap.dumping_machine.formaters;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;

public class AvroExtendedMessage {
    ConsumerRecord<String, GenericRecord> record;

    public AvroExtendedMessage(ConsumerRecord<String, GenericRecord> record) {
        this.record = record;
    }

    public int getPartition() {
        return this.record.partition();
    }

    public long getOffset() {
        return this.record.offset();
    }

    public long getTimestamp() {
        return this.record.timestamp();
    }

    public GenericRecord getRecord() {
        GenericRecordBuilder newGenericRecordBuilder = new GenericRecordBuilder(this.getSchema());

        newGenericRecordBuilder.set("metadata", this.getMetadata());

        for(Schema.Field field : this.record.value().getSchema().getFields()) {
            newGenericRecordBuilder.set(field.name(), this.record.value().get(field.name()));
        }

        return newGenericRecordBuilder.build();
    }

    public GenericRecord getMetadata() {
        GenericRecordBuilder newGenericRecordBuilder = new GenericRecordBuilder(this.getMetadataSchema());

        newGenericRecordBuilder.set("id", this.record.key());
        newGenericRecordBuilder.set("offset", this.record.offset());
        newGenericRecordBuilder.set("partition", this.record.partition());
        newGenericRecordBuilder.set("timestamp", this.record.timestamp());

        return newGenericRecordBuilder.build();
    }

    public Schema getSchema() {
        int position = 1;

        Schema schema = this.record.value().getSchema();
        List<Schema.Field> fields = schema.getFields();

        ArrayList<Schema.Field> newFields = new ArrayList();

        Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        newFields.add(0, new Schema.Field("metadata", Schema.createUnion(this.getMetadataSchema(), Schema.create(Schema.Type.NULL)), "", "null"));

        for(Schema.Field field : fields) {
            newFields.add(position, new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
            position++;
        }

        newSchema.setFields(newFields);

        return newSchema;
    }

    public Schema getMetadataSchema() {
        ArrayList<Schema.Field> fields = new ArrayList();

        Schema schema = Schema.createRecord("KafkaMetadata", "Kafka Metadata", "com.grupozap.dumping_machine", false);

        fields.add(new Schema.Field("id", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "ID", "null"));
        fields.add(new Schema.Field("offset", Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL)), "Offset", "null"));
        fields.add(new Schema.Field("partition", Schema.createUnion(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.NULL)), "Partition", "null"));
        fields.add(new Schema.Field("timestamp", Schema.createUnion(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.NULL)), "Timestamp", "null"));

        schema.setFields(fields);

        return schema;
    }
}
