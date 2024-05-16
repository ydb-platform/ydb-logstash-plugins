package tech.ydb.logstash;

import java.net.UnknownHostException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.Event;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.test.junit5.GrpcTransportExtension;
import tech.ydb.test.junit5.YdbHelperExtension;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.SyncReader;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

public class YdbTopicsOutputTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private final static GrpcTransportExtension transport = new GrpcTransportExtension();

    // some fixed timestamps for tests
    private static final Instant TS1 = Instant.ofEpochMilli(1581601437567L); // 2020-02-13 12:43:57.567 UTC
    private static final Instant TS2 = Instant.ofEpochMilli(1581601497120L); // 2020-02-13 12:44:57.120 UTC
    private static final Instant TS3 = Instant.ofEpochMilli(1581601498341L); // 2020-02-13 12:44:58.341 UTC

    private static Map<String, Object> createConfigMap() {
        Map<String, Object> config = new HashMap<>();
        String connectionString = ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();

        config.put(YdbTopicsOutput.CONNECTION.name(), connectionString);
        if (ydb.authToken() != null) {
            config.put(YdbTopicsOutput.TOKEN_AUTH.name(), ydb.authToken());
        }

        return config;
    }

    @Test
    public void basicTest() throws InterruptedException, UnknownHostException {
        String topicPath = ydb.database() + "/test/simple-topic";

        // create topic
        TopicClient client = TopicClient.newClient(transport).build();
        client.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName("test_consumer").build())
                .build())
                .join().expectSuccess("cannot create topic");
        try {
            SyncReader reader = client.createSyncReader(ReaderSettings.newBuilder()
                    .setConsumerName("test_consumer")
                    .addTopic(TopicReadSettings.newBuilder().setPath(topicPath).build())
                    .build());
            reader.init();

            Map<String, Object> config = createConfigMap();
            config.put(YdbTopicsOutput.TOPIC_PATH.name(), topicPath);

            YdbTopicsOutput plugin = new YdbTopicsOutput("test-simple", new ConfigurationImpl(config), null);

            Event ev1 = new org.logstash.Event();
            ev1.setEventTimestamp(TS1);
            ev1.setField("device", "dev1");
            ev1.setField("value", 1.5d);
            ev1.setField("priority", 1);

            Event ev2 = new org.logstash.Event();
            ev2.setEventTimestamp(TS2);
            ev2.setField("device", "dev2");
            ev2.setField("priority", -3);

            Event ev3 = new org.logstash.Event();
            ev3.setEventTimestamp(TS3);
            ev3.setField("device", "dev3");
            ev3.setField("value", -1f);
            ev3.setField("priority", 2);

            plugin.output(Arrays.asList(ev1, ev3, ev2));

            Message m1 = reader.receive(1, TimeUnit.SECONDS);
            Message m2 = reader.receive(1, TimeUnit.SECONDS);
            Message m3 = reader.receive(1, TimeUnit.SECONDS);

            Assertions.assertNotNull(m1);
            Assertions.assertNotNull(m2);
            Assertions.assertNotNull(m3);

            Assertions.assertEquals(
                    "{\"device\":\"dev1\",\"priority\":1,\"timestamp\":1581601437567,\"value\":1.5}",
                    new String(m1.getData())
            );
            Assertions.assertEquals(
                    "{\"device\":\"dev3\",\"priority\":2,\"timestamp\":1581601498341,\"value\":-1.0}",
                    new String(m2.getData())
            );
            Assertions.assertEquals(
                    "{\"device\":\"dev2\",\"priority\":-3,\"timestamp\":1581601497120}",
                    new String(m3.getData())
            );

            plugin.stop();
            plugin.awaitStop();

            reader.shutdown();
        } finally {
            // drop topic
            client.dropTopic(topicPath).join().expectSuccess("cannot drop topic");
        }
    }
}
