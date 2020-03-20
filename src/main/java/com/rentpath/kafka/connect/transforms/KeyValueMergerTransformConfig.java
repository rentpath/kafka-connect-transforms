package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class KeyValueMergerTransformConfig extends AbstractConfig {
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record key to merge into the value";

    public List<String> fields;

    public KeyValueMergerTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC);
    }
}
