package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class StringSplitTransformConfig extends AbstractConfig {
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record containing strings to split";

    public static final String DELIMITER_CONF = "delimiter";
    static final String DELIMITER_DOC = "The regular expression pattern to be used to split the string into a list";
    static final String DELIMITER_DEFAULT = ",";

    public static final String ELEMENT_NULLIFY_EMPTY_CONF = "element.nullify.empty";
    static final String ELEMENT_NULLIFY_EMPTY_DOC = "Whether empty elements in the split list (e.g. the second value of \"1,,2\") ought to be interpreted as null. If false (default), will interpret as empty string";
    static final boolean ELEMENT_NULLIFY_EMPTY_DEFAULT = false;

    public static final String NULLIFY_EMPTY_CONF = "nullify.empty";
    static final String NULLIFY_EMPTY_DOC = "Whether an empty string source value should be interpreted as null or an empty list (default)";
    static final boolean NULLIFY_EMPTY_DEFAULT = false;

    public static final String LISTIFY_NULL_CONF = "listify.null";
    static final String LISTIFY_NULL_DOC = "Whether a null sourc value should be interpreted as an empty list or null (default)";
    static final boolean LISTIFY_NULL_DEFAULT = false;

    public final List<String> fields;
    public final String delimiter;
    public final boolean elementNullifyEmpty;
    public final boolean nullifyEmpty;
    public final boolean listifyNull;

    public StringSplitTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.delimiter = getString(DELIMITER_CONF);
        this.elementNullifyEmpty = getBoolean(ELEMENT_NULLIFY_EMPTY_CONF);
        this.nullifyEmpty = getBoolean(NULLIFY_EMPTY_CONF);
        this.listifyNull = getBoolean(LISTIFY_NULL_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(DELIMITER_CONF, ConfigDef.Type.STRING, DELIMITER_DEFAULT, ConfigDef.Importance.HIGH, DELIMITER_DOC)
                .define(ELEMENT_NULLIFY_EMPTY_CONF, ConfigDef.Type.BOOLEAN, ELEMENT_NULLIFY_EMPTY_DEFAULT, ConfigDef.Importance.HIGH, ELEMENT_NULLIFY_EMPTY_DOC)
                .define(NULLIFY_EMPTY_CONF, ConfigDef.Type.BOOLEAN, NULLIFY_EMPTY_DEFAULT, ConfigDef.Importance.HIGH, NULLIFY_EMPTY_DOC)
                .define(LISTIFY_NULL_CONF, ConfigDef.Type.BOOLEAN, LISTIFY_NULL_DEFAULT, ConfigDef.Importance.HIGH, LISTIFY_NULL_DOC);
    }
}
