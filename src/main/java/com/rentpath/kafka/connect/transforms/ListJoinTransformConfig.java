package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class ListJoinTransformConfig extends AbstractConfig {
    public static final String FIELD_LIST_CONF = "field.list";
    static final String FIELD_LIST_DOC = "The field in the source record that contains the list to join";

    public static final String DELIMITER_CONF = "delimiter";
    static final String DELIMITER_DOC = "The delimiter to be used to join the elements of the list";
    static final String DELIMITER_DEFAULT = ",";

    public final String fieldList;
    public final String delimiter;

    public ListJoinTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fieldList = getString(FIELD_LIST_CONF);
        this.delimiter = getString(DELIMITER_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELD_LIST_CONF, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, FIELD_LIST_DOC)
                .define(DELIMITER_CONF, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, DELIMITER_DOC);
    }
}

