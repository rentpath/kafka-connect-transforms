package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
 This transform is used to take the raw bytes from a message with ByteArray value and wrap it in a struct with the
 original value represented as a single field in that struct (as configured by the `field` parameter), such that it
 may be consumed by connectors that _require_ structs such as the JDBC Sink Connector.
*/
public class UnionResolverTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(UnionResolverTransform.class);

    private UnionResolverTransformConfig config;

    private Schema resolveTypeSchema(String type, boolean isOptional) {
        SchemaBuilder builder = null;
        switch (type) {
            case "int":
                builder = SchemaBuilder.int32();
            case "long":
                builder = SchemaBuilder.int64();
            case "float":
                builder = SchemaBuilder.float32();
            case "double":
                builder = SchemaBuilder.float64();
        }
        if (builder != null) {
            if (isOptional)
                return builder.optional().build();
            else
                return builder.build();
        }
        return null;
    }

    @Override
    public R apply(R record) {
        if (record.value() == null)
            return record;
        if (null == record.valueSchema() || Schema.Type.STRUCT != record.valueSchema().type()) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        Struct inputRecord = (Struct) record.value();
        Schema inputSchema = inputRecord.schema();

        final SchemaBuilder builder = SchemaBuilder.struct();
        if (inputSchema.name() != null && !inputSchema.name().equals("")) {
            builder.name(inputSchema.name());
        }
        if (inputSchema.isOptional()) {
            builder.optional();
        }
        for (Field field : inputSchema.fields()) {
            final Schema fieldSchema;
            if (this.config.fields.contains(field.name()) && field.schema().type() == Schema.Type.STRUCT) {
                Struct union = inputRecord.getStruct(field.name());
                String selectedPriorityType = null;
                Object priorityValue;
                for (String priorityType : config.resolutionPriorities) {
                    priorityValue = union.get(priorityType);
                    if (priorityValue != null) {
                        selectedPriorityType = priorityType;
                        break;
                    }
                }
                if (selectedPriorityType != null) {
                    fieldSchema = resolveTypeSchema(selectedPriorityType, field.schema().isOptional());
                } else {
                    fieldSchema = resolveTypeSchema(config.resolutionPriorities.get(0), field.schema().isOptional());
                }
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : inputSchema.fields()) {
            if (this.config.fields.contains(field.name()) && field.schema().type() == Schema.Type.STRUCT) {
                switch (schema.field(field.name()).schema().type()) {
                    case INT32:
                        struct.put(field.name(), inputRecord.getStruct(field.name()).getInt32("int"));
                        break;
                    case INT64:
                        struct.put(field.name(), inputRecord.getStruct(field.name()).getInt64("long"));
                        break;
                    case FLOAT32:
                        struct.put(field.name(), inputRecord.getStruct(field.name()).getFloat32("float"));
                        break;
                    case FLOAT64:
                        struct.put(field.name(), inputRecord.getStruct(field.name()).getFloat64("double"));
                        break;
                }
            } else {
                struct.put(field.name(), inputRecord.get(field.name()));
            }
        }
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                struct.schema(),
                struct,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return UnionResolverTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new UnionResolverTransformConfig(map);
    }
}
