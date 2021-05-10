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

    public static final String NULLED_DEFAULT_FIELDS_CONF = "nulled.default.fields";
    static final String NULLED_DEFAULT_FIELDS_DOC = "A comma-delimited list of field names that should be set to null if the corresponding key interpreted by the pattern into that fieldname is not found in the map";

    public static final String NULLED_DEFAULT_TYPE_CONF = "nulled.default.type";
    static final String NULLED_DEFAULT_TYPE_DOC = "The type to set the nulled default fields, if specified. One of string, int, long, float, double, boolean, or bytes. Defaults to string.";

    public final List<String> fields;
    public final String pattern;
    public final List<String> nulledDefaultFields;
    public final String nulledDefaultType;

    public MapFlattenTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.pattern = getString(PATTERN_CONF);
        this.nulledDefaultFields = getList(NULLED_DEFAULT_FIELDS_CONF);
        this.nulledDefaultType = getString(NULLED_DEFAULT_TYPE_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(PATTERN_CONF, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, PATTERN_DOC)
                .define(NULLED_DEFAULT_FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, NULLED_DEFAULT_FIELDS_DOC)
                .define(NULLED_DEFAULT_TYPE_CONF, ConfigDef.Type.STRING, "string", ConfigDef.Importance.HIGH, NULLED_DEFAULT_TYPE_DOC);
    }
}

