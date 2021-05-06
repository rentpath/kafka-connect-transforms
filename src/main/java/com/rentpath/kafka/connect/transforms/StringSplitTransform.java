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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StringSplitTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(StringSplitTransform.class);

    private StringSplitTransformConfig config;

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
            if (this.config.fields.contains(field.name())) {
                SchemaBuilder listBuilder = SchemaBuilder.array(
                        config.elementNullifyEmpty ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA
                );
                if (field.schema().isOptional())
                    listBuilder.optional();
                fieldSchema = listBuilder.build();
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            if (this.config.fields.contains(field.name())) {
                String v = inputRecord.getString(field.name());
                List<String> list = null;
                if ((v == null && config.listifyNull) || (v != null && v.equals("") && !config.nullifyEmpty))
                    list = new ArrayList<>();
                else if (v != null && !v.equals("")) {
                    list = new ArrayList<>();
                    String[] originalStrs = v.split(config.delimiter);
                    for (String element : originalStrs)
                        if (element.equals("") && config.elementNullifyEmpty)
                            list.add(null);
                        else
                            list.add(element);
                }
                struct.put(field.name(), list);
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
        return ListJoinTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new StringSplitTransformConfig(map);
    }
}
