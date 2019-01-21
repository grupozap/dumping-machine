package com.grupozap.dumping_machine.deserializers;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

public class AvroSchemaRegistryDeserializer extends KafkaAvroDeserializer {
    private final Logger logger = LoggerFactory.getLogger(AvroSchemaRegistryDeserializer.class);

    public AvroSchemaRegistryDeserializer() {
        super();
    }

    public AvroSchemaRegistryDeserializer(SchemaRegistryClient client) {
        super(client);
    }

    public AvroSchemaRegistryDeserializer(SchemaRegistryClient client, Map<String, ?> props) {
        super(client, props);
    }

    public Object deserialize(String s, byte[] bytes) {
        try {
            return this.deserialize(bytes);
        } catch(SerializationException e) {
            logger.warn(e.getMessage());

            GenericRecordBuilder newGenericRecordBuilder = new GenericRecordBuilder(this.getSchema());

            newGenericRecordBuilder.set("message", e.toString());
            newGenericRecordBuilder.set("cause", e.getCause().toString());

            return newGenericRecordBuilder.build();
        }
    }

    public Schema getSchema() {
        ArrayList<Schema.Field> fields = new ArrayList();

        Schema schema = Schema.createRecord("KafkaException", "Kafka Exception", "com.grupozap.dumping_machine", false);

        fields.add(new Schema.Field("message", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "Message", "null"));
        fields.add(new Schema.Field("cause", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "Cause", "null"));

        schema.setFields(fields);

        return schema;
    }
}
