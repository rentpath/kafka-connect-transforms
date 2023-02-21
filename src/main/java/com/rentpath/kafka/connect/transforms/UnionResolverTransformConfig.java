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
    public static final String FIELDS_CONF = "fields";
    public static final String RESOLUTION_PRIORITIES = "resolution.priorities";
    static final String FIELDS_DOC = "Comma-delimited list of fields to apply the union resolution to";
    static final String RESOLUTION_PRIORITIES_DOC = "Comma-delimited list of types to use from the union in order of priority";

    public final List<String> fields;
    public final List<String> resolutionPriorities;

    public UnionResolverTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.fields = getList(FIELDS_CONF);
        this.resolutionPriorities = getList(RESOLUTION_PRIORITIES);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, FIELDS_DOC)
                .define(RESOLUTION_PRIORITIES, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, RESOLUTION_PRIORITIES_DOC);
    }
}

