package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class BoolStringTransformConfig extends AbstractConfig {
    public static final String COERCION_TYPE_TRUEFALSE = "truefalse";
    public static final String COERCION_TYPE_ONEZERO = "onezero";
    public static final String COERCION_TYPE_YESNO = "yesno";

    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record that contain booleans to coerce";

    public static final String COERCION_TYPE_CONF = "coercion.type";
    static final String COERCION_TYPE_DOC = "The type of string coercion to apply to the booleans";
    static final String COERCION_TYPE_DEFAULT = COERCION_TYPE_TRUEFALSE;

    public static final String COERCION_NULLIFY_FALSE_CONF = "coercion.nullify.false";
    static final String COERCION_NULLIFY_FALSE_DOC = "Will result in any logical false values coerced to null";
    static final boolean COERCION_NULLIFY_FALSE_DEFAULT = false;

    public static final String COERCION_CAPITALIZE_CONF = "coercion.capitalize";
    static final String COERCION_CAPITALIZE_DOC = "Ensures the output string is all capitals";
    static final boolean COERCION_CAPITALIZE_DEFAULT = false;

    public List<String> fields;
    public String coercionType = null;
    public boolean coercionNullifyFalse = false;
    public boolean coercionCapitalize = false;

    public BoolStringTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.coercionType = getString(COERCION_TYPE_CONF);
        this.coercionNullifyFalse = getBoolean(COERCION_NULLIFY_FALSE_CONF);
        this.coercionCapitalize = getBoolean(COERCION_CAPITALIZE_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(COERCION_TYPE_CONF, ConfigDef.Type.STRING, COERCION_TYPE_DEFAULT, ConfigDef.Importance.HIGH, COERCION_TYPE_DOC)
                .define(COERCION_NULLIFY_FALSE_CONF, ConfigDef.Type.BOOLEAN, COERCION_NULLIFY_FALSE_DEFAULT, ConfigDef.Importance.HIGH, COERCION_NULLIFY_FALSE_DOC)
                .define(COERCION_CAPITALIZE_CONF, ConfigDef.Type.BOOLEAN, COERCION_CAPITALIZE_DEFAULT, ConfigDef.Importance.HIGH, COERCION_CAPITALIZE_DOC);
    }
}

