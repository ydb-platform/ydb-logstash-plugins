package tech.ydb.logstash;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.test.junit5.GrpcTransportExtension;
import tech.ydb.test.junit5.YdbHelperExtension;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.Consumer;
import tech.ydb.topic.settings.CreateTopicSettings;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.SyncWriter;

public class YdbTopicsInputTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private final static GrpcTransportExtension transport = new GrpcTransportExtension();

    private final static Message RAW_M1 = Message.of(new byte[] { 0x00, 0x02, 0x01 });
    private final static Message RAW_M2 = Message.of(new byte[] { });
    private final static Message RAW_M3 = Message.of(new byte[] { 0x01, 0x02, -1, -2 });

    private final static Message JSON_M1 = Message.of("{ \"text\": \"ts\", \"number\": 1.4, \"b\": true }".getBytes());
    private final static Message JSON_M2 = Message.of("{}".getBytes());
    private final static Message JSON_M3 = Message.of("{ \"inner\": { \"number\": 1.4, \"b\": true }  }".getBytes());

    private static Map<String, Object> createConfigMap() {
        Map<String, Object> config = new HashMap<>();
        String connectionString = ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();

        config.put(YdbTopic.CONNECTION.name(), connectionString);
        if (ydb.authToken() != null) {
            config.put(YdbTopic.TOKEN_AUTH.name(), ydb.authToken());
        }

        return config;
    }

    @Test
    public void rawMessagesTest() throws InterruptedException, UnknownHostException {
        String topicPath = ydb.database() + "/test/simple-topic";
        String consumerName = "test-consumer";

        // create topic
        TopicClient client = TopicClient.newClient(transport).build();
        client.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName(consumerName).build())
                .build())
                .join().expectSuccess("cannot create topic");
        try {
            SyncWriter writer = client.createSyncWriter(WriterSettings.newBuilder()
                    .setTopicPath(topicPath)
                    .setCodec(Codec.RAW)
                    .build());
            writer.init();

            writer.send(RAW_M1);
            writer.send(RAW_M2);
            writer.send(RAW_M3);

            Map<String, Object> config = createConfigMap();
            config.put(YdbTopic.TOPIC_PATH.name(), topicPath);
            config.put(YdbTopic.CONSUMER_NAME.name(), consumerName);

            YdbTopic plugin = new YdbTopic("test-simple", new ConfigurationImpl(config), null);
            BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(10);
            plugin.start(queue::add);

            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M1);
            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M2);
            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M3);

            writer.send(RAW_M3);
            writer.send(RAW_M1);
            writer.send(RAW_M2);
            writer.flush();

            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M3);
            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M1);
            assertRawMessage(queue.poll(1, TimeUnit.SECONDS), RAW_M2);

            plugin.stop();
            plugin.awaitStop();
        } finally {
            // drop topic
            client.dropTopic(topicPath).join().expectSuccess("cannot drop topic");
        }
    }

    private void assertRawMessage(Map<String, Object> map, Message msg) {
        Assertions.assertTrue(map.containsKey("base64"));
        Object eventData = map.get("base64");
        Assertions.assertTrue(eventData instanceof String);
        Assertions.assertArrayEquals(Base64.getDecoder().decode((String) eventData), msg.getData());
    }

    @Test
    public void jsonMessagesTest() throws InterruptedException, UnknownHostException {
        String topicPath = ydb.database() + "/test/json-topic";
        String consumerName = "test-consumer";

        // create topic
        TopicClient client = TopicClient.newClient(transport).build();
        client.createTopic(topicPath, CreateTopicSettings.newBuilder()
                .addConsumer(Consumer.newBuilder().setName(consumerName).build())
                .build())
                .join().expectSuccess("cannot create topic");
        try {
            SyncWriter writer = client.createSyncWriter(WriterSettings.newBuilder()
                    .setTopicPath(topicPath)
                    .setCodec(Codec.RAW)
                    .build());
            writer.init();

            writer.send(JSON_M1);
            writer.send(JSON_M2);
            writer.send(JSON_M3);

            Map<String, Object> config = createConfigMap();
            config.put(YdbTopic.TOPIC_PATH.name(), topicPath);
            config.put(YdbTopic.CONSUMER_NAME.name(), consumerName);
            config.put(YdbTopic.SCHEMA.name(), "JSON");

            YdbTopic plugin = new YdbTopic("test-json", new ConfigurationImpl(config), null);
            BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(10);
            plugin.start(queue::add);

            assertJsonMessage1(queue.poll(1, TimeUnit.SECONDS));
            assertJsonMessage2(queue.poll(1, TimeUnit.SECONDS));
            assertJsonMessage3(queue.poll(1, TimeUnit.SECONDS));

            writer.send(RAW_M1); // will be ignored
            writer.send(JSON_M3);
            writer.send(JSON_M1);
            writer.send(JSON_M2);
            writer.flush();

            assertJsonMessage3(queue.poll(1, TimeUnit.SECONDS));
            assertJsonMessage1(queue.poll(1, TimeUnit.SECONDS));
            assertJsonMessage2(queue.poll(1, TimeUnit.SECONDS));

            plugin.stop();
            plugin.awaitStop();
        } finally {
            // drop topic
            client.dropTopic(topicPath).join().expectSuccess("cannot drop topic");
        }

    }

    private void assertJsonMessage1(Map<String, Object> map) {
        Assertions.assertTrue(map.containsKey("text"));
        Assertions.assertTrue(map.containsKey("number"));
        Assertions.assertTrue(map.containsKey("b"));

        Object text = map.get("text");
        Object number = map.get("number");
        Object b = map.get("b");

        Assertions.assertTrue(text instanceof String);
        Assertions.assertTrue(number instanceof Double);
        Assertions.assertTrue(b instanceof Boolean);

        Assertions.assertEquals("ts", (String) text);
        Assertions.assertEquals(Double.valueOf(1.4d), (Double) number);
        Assertions.assertEquals(Boolean.TRUE, (Boolean) b);
    }

    private void assertJsonMessage2(Map<String, Object> map) {
        Assertions.assertTrue(map.isEmpty());
    }

    private void assertJsonMessage3(Map<String, Object> map) {
        Assertions.assertTrue(map.containsKey("inner"));
        Object inner = map.get("inner");
        Assertions.assertTrue(inner instanceof String);
        Assertions.assertEquals("{\"number\":1.4,\"b\":true}", (String) inner);
    }
}