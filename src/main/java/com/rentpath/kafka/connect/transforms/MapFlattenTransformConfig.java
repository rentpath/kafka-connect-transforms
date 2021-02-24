package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class MapFlattenTransformConfig extends AbstractConfig {
    // example: "byBedroom"
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record with maps to flatten";

    // example: "bed%k_pricedrop"
    public static final String PATTERN_CONF = "pattern";
    static final String PATTERN_DOC = "The pattern with which to construct the flattened field names, with the substring '%k' replaced with the map's key";

    public final List<String> fields;
    public final String pattern;

    public MapFlattenTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.pattern = getString(PATTERN_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(PATTERN_CONF, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, PATTERN_DOC);
    }
}

