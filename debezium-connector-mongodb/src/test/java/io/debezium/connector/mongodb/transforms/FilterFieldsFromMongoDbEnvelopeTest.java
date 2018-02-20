/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mongodb.transforms;

import io.debezium.connector.mongodb.CollectionId;
import io.debezium.connector.mongodb.RecordMakers;
import io.debezium.connector.mongodb.RecordMakers.RecordsForCollection;
import io.debezium.connector.mongodb.SourceInfo;
import io.debezium.connector.mongodb.TopicSelector;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.print.Doc;
import java.util.*;

import static org.fest.assertions.Assertions.assertThat;

/**
 * Unit test for {@link FilterFieldsFromMongoDbEnvelope}. It uses {@link RecordMakers}
 * to assemble source records as the connector would emit them and feeds them to
 * the SMT.
 *
 * @author Raimondas Tijunaitis
 */
public class FilterFieldsFromMongoDbEnvelopeTest {

    private static final String SERVER_NAME = "serverX.";
    private static final String PREFIX = SERVER_NAME + ".";

    private SourceInfo source;
    private RecordMakers recordMakers;
    private TopicSelector topicSelector;
    private List<SourceRecord> produced;

    private FilterFieldsFromMongoDbEnvelope<SourceRecord> transformation;

    @Before
    public void setup() {
        source = new SourceInfo(SERVER_NAME);
        topicSelector = TopicSelector.defaultSelector(PREFIX);
        produced = new ArrayList<>();
        recordMakers = new RecordMakers(source, topicSelector, produced::add);
        transformation = new FilterFieldsFromMongoDbEnvelope();
    }

    @After
    public void closeSmt() {
        transformation.close();
    }

    @Test
    public void shouldTransformRecordRemovingBlacklistedFieldForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6));

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1));

        Map<String, String> cfg = new HashMap();
        cfg.put("blacklist", "nest");

        compareEvents(after, before, cfg, "after");
    }

    @Test
    public void shouldTransformRecordRemovingBlacklistedNestedFieldForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p2", 2));

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1).append("p2", 2));

        Map<String, String> cfg = new HashMap();
        cfg.put("blacklist", "nest.p1");

        compareEvents(after, before, cfg, "after");
    }

    @Test
    public void shouldTransformRecordRemovingBlacklistedAllNestedFieldForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document());

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1).append("p2", 2));

        Map<String, String> cfg = new HashMap();
        cfg.put("blacklist", "nest.p1,nest.p2");

        compareEvents(after, before, cfg, "after");
    }

    @Test
    public void shouldTransformRecordRemovingBlacklistedMultiNestedFieldForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1).append("p2", new Document().append("pp2", 2)));

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1).append("p2", new Document().append("pp1", 1).append("pp2", 2)));

        Map<String, String> cfg = new HashMap();
        cfg.put("blacklist", "nest.p2.pp1");

        compareEvents(after, before, cfg, "after");
    }

    @Test
    public void shouldTransformRecordSelectingWhitelistedFieldOnlyForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally");

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("nest", new Document().append("p1", 1));

        Map<String, String> cfg = new HashMap();
        cfg.put("whitelist", "_id,name");

        compareEvents(after, before, cfg, "after");
    }

    @Test
    public void shouldTransformRecordSelectingWhitelistedFieldsFromArraysForInsertEvent() throws InterruptedException {
        ObjectId id = new ObjectId();
        Document after = new Document().append("_id", id)
                .append("name", "Sally")
                .append("events", Arrays.asList(
                        new Document().append("e1", "1"),
                        new Document().append("e1", "12")));

        Document before = new Document().append("_id", id)
                .append("name", "Sally")
                .append("phone", 123L)
                .append("active", true)
                .append("scores", Arrays.asList(1.2, 3.4, 5.6))
                .append("events", Arrays.asList(
                        new Document().append("e1", "1").append("e2", "2"),
                        new Document().append("e1", "12").append("e2", "22")));

        Map<String, String> cfg = new HashMap();
        cfg.put("whitelist", "_id,name,events,events[].e1");

        compareEvents(after, before, cfg, "after");
    }

    private void compareEvents(Document doc1, Document doc2, Map<String, String> cfg, String docName) throws InterruptedException {

        SourceRecord recordOrig = createSourceRecord(doc1);
        SourceRecord record = createSourceRecord(doc2);

        // when
        transformation.configure(cfg);

        SourceRecord transformed = transformation.apply(record);
        String json1 = getDocumentJson(recordOrig, docName);
        String json2 = getDocumentJson(transformed, docName);
        assertThat(json1).isNotNull().isNotEmpty();
        assertThat(json2).isNotNull().isNotEmpty();
        assertThat(json2).isEqualTo(json1);

        transformation.close();
    }

    private String getDocumentJson(SourceRecord record, String field) {
        return  ((Struct) record.value()).getString(field);
    }


    private SourceRecord createSourceRecord(Document obj) throws InterruptedException {
        CollectionId collectionId = new CollectionId("rs0", "dbA", "c1");
        BsonTimestamp ts = new BsonTimestamp(1000, 1);

        Document event = new Document().append("o", obj)
                                       .append("ns", "dbA.c1")
                                       .append("ts", ts)
                                       .append("h", Long.valueOf(12345678))
                                       .append("op", "i");
        RecordsForCollection records = recordMakers.forCollection(collectionId);
        records.recordEvent(event, 1002);
        return produced.get(produced.size()-1);
    }
}
