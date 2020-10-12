package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StringTruncateTransformConfig extends AbstractConfig {
    public static final String FIELDLENGTHS_CONF = "fieldlengths";
    static final String FIELDLENGTHS_DOC = "Comma-delimited list of pairs of fields and maximum lengths in the source record, of the format 'field:length'.";

    public final Map<String,Integer> fieldLengths;

    public StringTruncateTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        List<String> fieldLengthStrings = getList(FIELDLENGTHS_CONF);
        fieldLengths = new HashMap<String, Integer>();
        String[] pair;
        for (String fieldLengthString : fieldLengthStrings) {
            pair = fieldLengthString.split(":");
            fieldLengths.put(pair[0], Integer.parseInt(pair[1]));
        }
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDLENGTHS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDLENGTHS_DOC);
    }
}
