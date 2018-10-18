package io.debezium.connector.mongodb.transforms;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.RecordMakers;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.connector.mongodb.TopicSelector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Unit test for {@link FallbackToLegacyMessageKeyFormat}. It uses {@link RecordMakers}
 * to assemble source records as the connector would emit them and feeds them to
 * the SMT.
 *
 * @author Raimondas Tijunaitis
 */

public class FallbackToLegacyMessageKeyFormatTest {
    private static final String SERVER_NAME = "serverX.";
    private static final String PREFIX = SERVER_NAME + ".";

    private SourceInfo source;
    private RecordMakers recordMakers;
    private TopicSelector topicSelector;
    private List<SourceRecord> produced;

    private FallbackToLegacyMessageKeyFormat<SourceRecord> transformation;

    @Before
    public void setup() {
        source = new SourceInfo(SERVER_NAME);
        topicSelector = TopicSelector.defaultSelector(PREFIX);
        produced = new ArrayList<>();
        recordMakers = new RecordMakers(source, topicSelector, produced::add);
        transformation = new FallbackToLegacyMessageKeyFormat();
    }

    @After
    public void closeSmt() {
        transformation.close();
    }

    @Test
    public void shouldMapKeyToLegacyDebeziumStructureTest() throws InterruptedException {

        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        SourceRecord record = createSourceRecord(after);

        assertThat(((Struct)record.key()).get("id")).isNotNull();
        assertThat(((Struct)record.key()).getString("id")).isNotEqualTo(id.toString());

        // when
        Map<String, String> cfg = new HashMap();

        transformation.configure(cfg);

        SourceRecord transformed = transformation.apply(record);
        assertThat(((Struct)transformed.key()).get("_id")).isNotNull();
        assertThat(((Struct)transformed.key()).getString("_id")).isEqualTo(id.toString());
        transformation.close();
    }


    private SourceRecord createSourceRecord(Document after) throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);

        Document event = new Document().append("o", after)
                .append("ns", "dbA.c1")
                .append("ts", ts)
                .append("h", Long.valueOf(12345678))
                .append("op", "i");
        RecordMakers.RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        return produced.get(produced.size()-1);
    }

}
