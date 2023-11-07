package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.List;
import java.util.Map;

/*
 This transform is used to set a target field depending on the first non-null value between itself and a list of contributing fields.
 */
public class PopulateFieldTransformConfig extends AbstractConfig {

    public static final String TARGET_FIELD_CONF = "target.field";
    static final String TARGET_FIELD_DOC = "The field name for the target field we want to set";

    public static final String CONTRIBUTOR_FIELDS_CONF = "contributor.fields";
    static final String CONTRIBUTOR_FIELDS_DOC = "A comma-delimited list of fields that contribute to setting the target field";


    public final String targetField;
    public final List<String> contributorFields;

    public PopulateFieldTransformConfig(Map<String, ?> parsedConfig) {
        super(config(), parsedConfig);
        this.targetField = getString(TARGET_FIELD_CONF);
        this.contributorFields = getList(CONTRIBUTOR_FIELDS_CONF);
    }

    static ConfigDef config() {
        return new ConfigDef()
                .define(TARGET_FIELD_CONF, ConfigDef.Type.STRING, null, ConfigDef.Importance.HIGH, TARGET_FIELD_DOC)
                .define(CONTRIBUTOR_FIELDS_CONF, ConfigDef.Type.LIST, null, ConfigDef.Importance.HIGH, CONTRIBUTOR_FIELDS_DOC);
    }
}