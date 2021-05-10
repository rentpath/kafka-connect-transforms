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

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapFlattenTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(MapFlattenTransform.class);

    private MapFlattenTransformConfig config;
    private final Pattern keyMatchPattern = Pattern.compile("%k");

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
        Matcher matcher = keyMatchPattern.matcher(this.config.pattern);
        for (Field field : inputSchema.fields()) {
            final Schema fieldSchema;
            Schema oldFieldSchema = field.schema();
            if (this.config.fields.contains(field.name()) && oldFieldSchema.type().equals(Schema.Type.MAP)) {
                Schema oldValueSchema = oldFieldSchema.valueSchema();
                if (inputRecord.getMap(field.name()) != null) {
                    for (Map.Entry<Object,Object> entry : inputRecord.getMap(field.name()).entrySet()) {
                        builder.field(matcher.replaceAll((String) entry.getKey()), oldValueSchema);
                    }
                }
            } else {
                fieldSchema = field.schema();
                builder.field(field.name(), fieldSchema);
            }
        }
        if (this.config.nulledDefaultFields != null) {
            Schema fieldSchema = null;
            switch (this.config.nulledDefaultType) {
                case "string":
                    fieldSchema = Schema.OPTIONAL_STRING_SCHEMA;
                    break;
                case "boolean":
                    fieldSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
                    break;
                case "int":
                    fieldSchema = Schema.OPTIONAL_INT32_SCHEMA;
                    break;
                case "long":
                    fieldSchema = Schema.OPTIONAL_INT64_SCHEMA;
                    break;
                case "float":
                    fieldSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
                    break;
                case "double":
                    fieldSchema = Schema.OPTIONAL_FLOAT64_SCHEMA;
                    break;
                case "bytes":
                    fieldSchema = Schema.OPTIONAL_BYTES_SCHEMA;
                    break;
            }
            for (String nulledField : this.config.nulledDefaultFields) {
                // if the field doesn't already exist by being set by the map...
                if (builder.field(nulledField) == null) {
                    // set it to the optional field type specified
                    builder.field(nulledField, fieldSchema);
                }
            }
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : inputSchema.fields()) {
            Schema oldFieldSchema = field.schema();
            if (this.config.fields.contains(field.name()) && oldFieldSchema.type().equals(Schema.Type.MAP)) {
                if (inputRecord.getMap(field.name()) != null) {
                    for (Map.Entry<Object, Object> entry : inputRecord.getMap(field.name()).entrySet()) {
                        struct.put(matcher.replaceAll((String) entry.getKey()), entry.getValue());
                    }
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
        return MapFlattenTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new MapFlattenTransformConfig(map);
    }
}
