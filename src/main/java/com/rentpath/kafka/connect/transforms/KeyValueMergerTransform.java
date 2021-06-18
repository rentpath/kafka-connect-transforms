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

public class KeyValueMergerTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(StringSplitTransform.class);
    private KeyValueMergerTransformConfig config;

    @Override
    public R apply(R record) {
        if (record.value() == null)
            return record;
        if (null == record.keySchema() || !(record.keySchema().type() == Schema.Type.STRUCT || record.keySchema().type().isPrimitive())) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not primitive nor is it a struct.");
            return record;
        }
        if (null == record.valueSchema() || record.valueSchema().type() != Schema.Type.STRUCT) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        final SchemaBuilder builder = SchemaBuilder.struct();
        Struct inputValueRecord = (Struct) record.value();
        Schema inputValueSchema = inputValueRecord.schema();
   
        if (inputValueSchema.name() != null && !inputValueSchema.name().equals("")) {
            builder.name(inputValueSchema.name());
        }
        if (inputValueSchema.isOptional()) {
            builder.optional();
        }
        // copy the input record value field schemas into the output SchemaBuilder
        for (Field field : inputValueSchema.fields()) {
            builder.field(field.name(), field.schema());
        }

        if (record.keySchema().type() == Schema.Type.STRUCT) {
            Struct inputKeyRecord = (Struct) record.key();
            Schema inputKeySchema = inputKeyRecord.schema();
            
            // add only the key fields into output SchemaBuilder that are passed via config
            List<Field> keyFields = new ArrayList<>();
            for (Field field : inputKeySchema.fields()) {
                if (this.config.fields.contains(field.name())) {
                    keyFields.add(field);
                    builder.field(field.name(), field.schema());
                }
            }

            // use builder to instantiate an output struct, then add all value and neceasary key fields from the record
            Schema schema = builder.build();
            Struct struct = new Struct(schema);
            for (Field field : inputValueSchema.fields()) {
                struct.put(field.name(), inputValueRecord.get(field.name()));
            }
            for (Field field : keyFields) {
                struct.put(field.name(), inputKeyRecord.get(field.name()));
            }

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                struct.schema(),
                struct,
                record.timestamp());
        }
        else {
            // primitive type so will only use the first value in config.fields as fieldName; add the key to output schemaBuilder
            String keyFieldName = this.config.fields.get(0);
            builder.field(keyFieldName, record.keySchema());

            // instantiate output struct, copy all record values into it
            Schema schema = builder.build();
            Struct struct = new Struct(schema);
            for (Field field : inputValueSchema.fields()) {
                struct.put(field.name(), inputValueRecord.get(field.name()));
            }

            // put the key into the output struct
            struct.put(keyFieldName, record.key());

            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                struct.schema(),
                struct,
                record.timestamp());
        }
    }

    @Override
    public ConfigDef config() {
        return KeyValueMergerTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new KeyValueMergerTransformConfig(map);
    }
}
