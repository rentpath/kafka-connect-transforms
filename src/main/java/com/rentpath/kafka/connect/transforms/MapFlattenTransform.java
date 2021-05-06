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
