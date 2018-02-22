package io.debezium.connector.mongodb.transforms;

import com.mongodb.util.JSON;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.bson.types.ObjectId;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * SMT to fallback to legacy debezium MongoDB key format that was deprecated (http://debezium.io/docs/releases/#release-0-6-0).
 * Use it to prevent consumers to break after debezium mongodb connector upgrade
 *
 * Usage example:
 *
 * ...
 * "transforms": "fallback_keys",
 * "transforms.fallback_keys.type": "io.debezium.connector.mongodb.transforms.FallbackToLegacyMessageKeyFormat",

 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Raimondas Tijunaitis
 */

public class FallbackToLegacyMessageKeyFormat<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String PURPOSE = "field replacement";
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public R apply(R record) {
        final Struct value = requireStruct(record.key(), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        Field field = updatedSchema.field("_id");
        final Struct updatedValue = new Struct(updatedSchema);
        final String fieldValue = value.getString("id");

        updatedValue.put(field.name(), objectIdLiteral(JSON.parse(fieldValue)));

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        schema.field("id");
        builder.field("_id", schema.field("id").schema());
        return builder.build();
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }

    protected String objectIdLiteral(Object id) {
        if (id == null) {
            return null;
        }
        if (id instanceof ObjectId) {
            return ((ObjectId) id).toHexString();
        }
        if (id instanceof String) {
            return (String) id;
        }
        return id.toString();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
    }
}
