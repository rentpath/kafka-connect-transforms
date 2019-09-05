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

public class BoolStringTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(BoolStringTransform.class);

    private BoolStringTransformConfig config;

    @Override
    public R apply(R record) {
        if (null == record.valueSchema() || Schema.Type.STRUCT != record.valueSchema().type()) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        Struct inputRecord = (Struct) record.value();
        Schema inputSchema = inputRecord.schema();

        try {
            final SchemaBuilder builder = SchemaBuilder.struct();
            if (inputSchema.name() != null && !inputSchema.name().equals("")) {
                builder.name(inputSchema.name());
            }
            if (inputSchema.isOptional()) {
                builder.optional();
            }
            for (Field field : inputSchema.fields()) {
                final Schema fieldSchema;
                if (this.config.fieldList.equals(field.name())) {
                    fieldSchema = field.schema().isOptional() ?
                            Schema.OPTIONAL_STRING_SCHEMA :
                            Schema.STRING_SCHEMA;
                } else {
                    fieldSchema = field.schema();
                }
                builder.field(field.name(), fieldSchema);
            }
            Schema schema = builder.build();
            Struct struct = new Struct(schema);
            for (Field field : schema.fields()) {
                if (this.config.fieldList.equals(field.name())) {
                    List<String> list = inputRecord.getArray(this.config.fieldList);
                    struct.put(field.name(), String.join(this.config.delimiter, list));
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
        } catch (Throwable e) {
            log.warn("Exception encountered while attempting to join list field", e);
            return null;
        }
    }

    @Override
    public ConfigDef config() {
        return BoolStringTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new BoolStringTransformConfig(map);
    }
}
