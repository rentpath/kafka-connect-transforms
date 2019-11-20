package com.rentpath.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class BoolCoercionTransform<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(BoolCoercionTransform.class);
    public static final Pattern PATTERN_TRUE = Pattern.compile("(?i)^true$");
    public static final Pattern PATTERN_FALSE = Pattern.compile("(?i)^false$");
    public static final Pattern PATTERN_ONE = Pattern.compile("^1$");
    public static final Pattern PATTERN_ZERO = Pattern.compile("^0$");
    public static final Pattern PATTERN_YES = Pattern.compile("(?i)^yes$");
    public static final Pattern PATTERN_NO = Pattern.compile("(?i)^no$");

    private BoolCoercionTransformConfig config;

    @Override
    public R apply(R record) {
        if (null == record.valueSchema() || Schema.Type.STRUCT != record.valueSchema().type()) {
            log.trace("record.valueSchema() is null or record.valueSchema() is not a struct.");
            return record;
        }

        Struct inputRecord = (Struct) record.value();
        Schema inputSchema = inputRecord.schema();

        final SchemaBuilder builder = SchemaBuilder.struct();
        if (inputSchema.name() != null && !inputSchema.name().equals("")) {
            builder.name(inputSchema.name());
        }
        if (inputSchema.isOptional()) {
            builder.optional();
        }
        for (Field field : inputSchema.fields()) {
            final Schema fieldSchema;
            if (this.config.fields.contains(field.name())) {
                fieldSchema = (field.schema().isOptional() || !config.coercionFalsifyNull || !config.coercionFalsifyEmpty) ?
                        Schema.OPTIONAL_BOOLEAN_SCHEMA :
                        Schema.BOOLEAN_SCHEMA;
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }
        Schema schema = builder.build();
        Struct struct = new Struct(schema);
        for (Field field : inputSchema.fields()) {
            if (this.config.fields.contains(field.name())) {
                if (inputRecord.get(field.name()) == null) {
                    if (config.coercionFalsifyNull)
                        struct.put(field.name(), false);
                    else
                        struct.put(field.name(), null);
                } else if (field.schema().type() == Schema.Type.STRING) {
                    String v = inputRecord.getString(field.name());
                    if (v.equals(""))
                        if (config.coercionFalsifyEmpty)
                            struct.put(field.name(), false);
                        else
                            struct.put(field.name(), null);
                    else if (PATTERN_FALSE.matcher(v).matches() ||
                            PATTERN_ZERO.matcher(v).matches() ||
                            PATTERN_NO.matcher(v).matches())
                        struct.put(field.name(), false);
                    else if (PATTERN_TRUE.matcher(v).matches() ||
                            PATTERN_ONE.matcher(v).matches() ||
                            PATTERN_YES.matcher(v).matches())
                        struct.put(field.name(), true);
                    else
                        throw new DataException(String.format("Field has non-boolean-interpretable string value: %s", v));
                } else if (field.schema().type() == Schema.Type.INT8 ||
                        field.schema().type() == Schema.Type.INT16 ||
                        field.schema().type() == Schema.Type.INT32 ||
                        field.schema().type() == Schema.Type.INT64) {
                    long longValue = (Long)inputRecord.get(field.name());
                    if (longValue == 0)
                        struct.put(field.name(), false);
                    else
                        struct.put(field.name(), true);
                } else if (field.schema().type() == Schema.Type.BOOLEAN)
                    struct.put(field.name(), inputRecord.getBoolean(field.name()));
            } else {
                struct.put(field.name(), inputRecord.get(field.name()));
            }
        }
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                struct.schema(),
                struct,
                record.timestamp()
        );
    }

    @Override
    public ConfigDef config() {
        return BoolStringTransformConfig.config();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        this.config = new BoolCoercionTransformConfig(map);
    }
}
