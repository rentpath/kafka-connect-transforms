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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StringTruncateTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(StringTruncateTransform.class);

    private StringTruncateTransformConfig config;

    @Override
    public R apply(R record) {
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
            builder.field(field.name(), field.schema());
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            if (this.config.fieldLengths.containsKey(field.name()) &&
                    field.schema().type() == Schema.Type.STRING) {
                String v = inputRecord.getString(field.name());
                Integer maxLength = this.config.fieldLengths.get(field.name());
                if (v.length() > maxLength)
                    struct.put(field.name(), v.substring(0, maxLength - 1));
                else
                    struct.put(field.name(), v);
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
        return StringTruncateTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new StringTruncateTransformConfig(map);
    }
}
