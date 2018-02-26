package io.debezium.connector.mongodb.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;


/**
 * SMT to filter MongoDB envelope content.
 *
 * Usage example:
 *
 * ...
 * "transforms": "filterfields",
 * "transforms.filterfields.type": "io.debezium.connector.mongodb.transforms.FilterFieldsFromMongoDbEnvelope",
 * "transforms.filterfields.whitelist": "a,nested.p1,array,array[].p1",
 * "transforms.filterfields.blacklist": "b"
 *
 * @param <R> the subtype of {@link ConnectRecord} on which this transformation will operate
 * @author Raimondas Tijunaitis
 */

public class FilterFieldsFromMongoDbEnvelope<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final JsonWriterSettings WRITER_SETTINGS = new JsonWriterSettings(JsonMode.STRICT, "", "");

    public static final String OVERVIEW_DOC = "Filter fields from MongoDB envelope in 'patch' and 'after' fields.";

    interface ConfigName {
        String BLACKLIST = "blacklist";
        String WHITELIST = "whitelist";
    }

    private List<String> blacklist;
    private List<String> whitelist;


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.BLACKLIST, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to exclude. This takes precedence over the whitelist.")
            .define(ConfigName.WHITELIST, ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM,
                    "Fields to include. If specified, only these fields will be used.");

    @Override
    public R apply(R r) {
        if (r.value() == null) {
            return r;
        }

        Struct doc = (Struct) r.value();

        if (doc.get("after") != null) {
            filterFields(doc, "after");
        }

        if (doc.get("patch") != null) {
            filterFields(doc, "patch");
        }

        return r;
    }

    private void filterFields(Struct doc, String docName) {
        BsonDocument val = BsonDocument.parse(doc.getString(docName));
        Set<Map.Entry<String, BsonValue>> set =  val.entrySet();
        filterFields(set, "");

        doc.put(docName, val.toJson(WRITER_SETTINGS));
    }

    private void filterFields(Set<Map.Entry<String, BsonValue>> set, String path) {
        for (Iterator<Map.Entry<String, BsonValue>> iterator = set.iterator(); iterator.hasNext(); ) {
            Map.Entry<String, BsonValue> item =  iterator.next();

            if (item.getValue().getClass().equals(BsonDocument.class)) {
                filterFields(((BsonDocument)item.getValue()).entrySet(), path + item.getKey() + ".");
            }

            if (item.getValue().getClass().equals(BsonArray.class)) {
                for (Iterator<BsonValue> iterator2 = ((BsonArray) item.getValue()).iterator(); iterator2.hasNext(); ) {
                    BsonValue element = iterator2.next();
                    if (element.getClass().equals(BsonDocument.class)) {
                        filterFields(((BsonDocument) element).entrySet(), path + item.getKey() + "[].");
                    }
                }
            }

            if (!filter(path + item.getKey())) {
                iterator.remove();
            }
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> map) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, map);
        blacklist = config.getList(ConfigName.BLACKLIST);
        whitelist = config.getList(ConfigName.WHITELIST);
    }

    boolean filter(String fieldName) {
        return !blacklist.contains(fieldName) && (whitelist.isEmpty() || whitelist.contains(fieldName));
    }
}
