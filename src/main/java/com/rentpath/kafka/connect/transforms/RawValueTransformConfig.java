package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class RawValueTransformConfig extends AbstractConfig {
    // example: "byBedroom"
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

