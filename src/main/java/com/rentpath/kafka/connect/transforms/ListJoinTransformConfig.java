package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class ListJoinTransformConfig extends AbstractConfig {
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record containing lists to join";

    public static final String DELIMITER_CONF = "delimiter";
    static final String DELIMITER_DOC = "The delimiter to be used to join the elements of the list";
    static final String DELIMITER_DEFAULT = ",";

    public final List<String> fields;
    public final String delimiter;

    public ListJoinTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.delimiter = getString(DELIMITER_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(DELIMITER_CONF, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, DELIMITER_DOC);
    }
}

