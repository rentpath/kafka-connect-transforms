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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*
 This transform is used to take the raw bytes from a message with ByteArray value and wrap it in a struct with the
 original value represented as a single field in that struct (as configured by the `field` parameter), such that it
 may be consumed by connectors that _require_ structs such as the JDBC Sink Connector.
*/
public class RawValueTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(RawValueTransform.class);

    private RawValueTransformConfig config;

    @Override
    public R apply(R record) {
        if (record.value() == null)
            return record;

        final SchemaBuilder builder = SchemaBuilder.struct();
        builder.field(this.config.field, Schema.BYTES_SCHEMA);
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        struct.put(this.config.field, record.value());
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
        return RawValueTransformConfig.config();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new RawValueTransformConfig(map);
    }
}
