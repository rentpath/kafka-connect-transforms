package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/*
 This transform, which gets its name and function from the 'Coalesce' SQL function, is used to set a target field based on the first non-null value between itself and a list of contributing fields.
 */
public class CoalesceFieldsTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(MapFlattenTransform.class);

    private CoalesceFieldsTransformConfig config;

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
            builder.field(field.name(), field.schema());
        }

        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : schema.fields()) {
            Object outValue = inputRecord.get(field.name());
            if (this.config.targetField.equals(field.name()) && outValue == null) {
                for (String contributorFieldName : this.config.contributorFields) {
                    outValue = inputRecord.get(contributorFieldName);
                    if (outValue != null) {
                        Schema.Type contributorFieldType = inputSchema.field(contributorFieldName).schema().type();
                        if (contributorFieldType == field.schema().type()) {
                            break;
                        } else {
                            throw new DataException(String.format("Fields are of different types. " +
                                                                  "Contributor field, %s, is of type %s, while Target field, %s, is of type %s",
                                                                  contributorFieldName, contributorFieldType.getName(),
                                                                  this.config.targetField, field.schema().type().getName()));
                        }
                    }
                }
            }
            struct.put(field.name(), outValue);
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
        return CoalesceFieldsTransformConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new CoalesceFieldsTransformConfig(map);
    }
}