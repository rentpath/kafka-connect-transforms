package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class KeyValueMergerTransformConfig extends AbstractConfig {
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "If attempting to merge a struct key these are the fields in the source record key to merge into the value. If attempting to merge a primitive key this will be the field name the key gets merged into the value under and only the first entry is used";

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
