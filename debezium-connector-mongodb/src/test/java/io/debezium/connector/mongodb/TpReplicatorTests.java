//package io.debezium.connector.mongodb;
//
//import io.debezium.config.Configuration;
//import io.debezium.util.Testing;
//import org.apache.kafka.connect.source.SourceRecord;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.util.LinkedList;
//import java.util.List;
//
//import static org.fest.assertions.Assertions.assertThat;
//
//
//public class TpReplicatorTests extends AbstractMongoIT {
//
//    @Before
//    public void beforeEach() {
//        Testing.Print.disable();
//        Testing.Debug.disable();
//        this.useConfiguration(TpReplicatorTests.getConfiguration());
//    }
//
//    @Test
//    public void shouldReturnVersion() throws InterruptedException {
//        assertThat(Module.version()).isNotNull();
//        assertThat(Module.version()).isNotEmpty();
//
//
//        List<SourceRecord> records = new LinkedList<>();
//        Replicator replicator = new Replicator(context, replicaSet, records::add, (x) -> {});
//        Thread thread = new Thread(replicator::run);
//        thread.start();
//
//        Thread.sleep(Integer.MAX_VALUE);
//    }
//
//    public static Configuration getConfiguration() {
//        return Configuration.fromSystemProperties("connector.").edit()
//                .withDefault(MongoDbConnectorConfig.HOSTS, "set-545a4ec660e6d158ff007ced/trustpilot-m1.tp-staging.com:27017")
//                .withDefault(MongoDbConnectorConfig.COLLECTION_WHITELIST, "Trustpilot.BusinessUnit")
//                .withDefault(MongoDbConnectorConfig.PASSWORD, "******")
//                .withDefault(MongoDbConnectorConfig.USER, "TP_OplogReader")
//                .withDefault(MongoDbConnectorConfig.AUTO_DISCOVER_MEMBERS, true)
//                .withDefault(MongoDbConnectorConfig.LOGICAL_NAME, "mongo.tp").build();
//    }
//
//}
