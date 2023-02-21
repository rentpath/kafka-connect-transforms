package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

/*
 This transform is used to take the raw bytes from a message with ByteArray value and wrap it in a struct with the
 original value represented as a single field in that struct (as configured by the `field` parameter), such that it
 may be consumed by connectors that _require_ structs such as the JDBC Sink Connector.
*/
public class UnionResolverTransformConfig extends AbstractConfig {
    // example: "content"
    public static final String FIELDS_CONF = "field";
    public static final String TYPE_PRIORITIES_CONF = "type.priorities";
    static final String FIELDS_DOC = "Comma-delimited list of fields to apply the union resolution to";
    static final String TYPE_PRIORITIES_DOC = "Comma-delimited list of types to use from the union in order of priority";

    public final List<String> fields;
    public final List<String> typePriorities;

    public UnionResolverTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.typePriorities = getList(TYPE_PRIORITIES_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(TYPE_PRIORITIES_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, TYPE_PRIORITIES_DOC);
    }
}

