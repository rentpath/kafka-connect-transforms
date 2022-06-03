package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

/*
 This transform is used to take the raw bytes from a message with ByteArray value and wrap it in a struct with the
 original value represented as a single field in that struct (as configured by the `field` parameter), such that it
 may be consumed by connectors that _require_ structs such as the JDBC Sink Connector.
*/
public class RawValueTransformConfig extends AbstractConfig {
    // example: "content"
    public static final String FIELD_CONF = "field";
    static final String FIELD_DOC = "The fieldname in the output schema that will hold the raw value";

    public final String field;

    public RawValueTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.field = getString(FIELD_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_CONF, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, FIELD_DOC);
    }
}

