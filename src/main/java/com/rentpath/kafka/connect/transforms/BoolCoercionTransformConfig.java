package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

public class BoolCoercionTransformConfig extends AbstractConfig {
    public static final String FIELDS_CONF = "fields";
    static final String FIELDS_DOC = "The fields in the source record that contain booleans to coerce";

    public static final String COERCION_FALSIFY_NULL_CONF = "coercion.falsify.null";
    static final String COERCION_FALSIFY_NULL_DOC = "Will a null value be interpreted as logical false or null (default)?";
    static final boolean COERCION_FALSIFY_NULL_DEFAULT = false;

    public static final String COERCION_FALSIFY_EMPTY_CONF = "coercion.falsify.null";
    static final String COERCION_FALSIFY_EMPTY_DOC = "Will an empty string value be interpreted as logical false or null (default)?";
    static final boolean COERCION_FALSIFY_EMPTY_DEFAULT = false;

    public List<String> fields;
    public boolean coercionFalsifyNull = false;
    public boolean coercionFalsifyEmpty = false;

    public BoolCoercionTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.coercionFalsifyNull = getBoolean(COERCION_FALSIFY_NULL_CONF);
        this.coercionFalsifyEmpty = getBoolean(COERCION_FALSIFY_EMPTY_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(COERCION_FALSIFY_NULL_CONF, ConfigDef.Type.BOOLEAN, COERCION_FALSIFY_NULL_DEFAULT, ConfigDef.Importance.HIGH, COERCION_FALSIFY_NULL_DOC)
                .define(COERCION_FALSIFY_EMPTY_CONF, ConfigDef.Type.BOOLEAN, COERCION_FALSIFY_EMPTY_DEFAULT, ConfigDef.Importance.HIGH, COERCION_FALSIFY_EMPTY_DOC);
    }
}

