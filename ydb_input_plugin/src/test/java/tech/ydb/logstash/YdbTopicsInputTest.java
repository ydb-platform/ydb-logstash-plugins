package tech.ydb.logstash;

import java.util.*;
import java.util.function.Consumer;

import co.elastic.logstash.api.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbTopicsInputTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private YdbTopicsInput input;

    private static String connectionString() {
        return ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();
    }

    @BeforeEach
    public void setUp() {
        String connectionString = connectionString();
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(YdbTopicsInput.PREFIX_CONFIG.name(), "message");
        configValues.put(YdbTopicsInput.EVENT_COUNT_CONFIG.name(), 1L);
        configValues.put("topic_path", "fake_topic_path");
        configValues.put("connection_string", connectionString);
        configValues.put("consumer_name", "consumer");
        configValues.put("schema", "JSON");
        Configuration config = new ConfigurationImpl(configValues);

        input = new YdbTopicsInput("test-input", config, null);
    }

    @AfterEach
    public void tearDown() {
        input.stop();
    }

    @Test
    public void testStart() {
        Map<String, Object> resultMap = new HashMap<>();
        Consumer<Map<String, Object>> consumer = stringObjectMap -> {
            for (String key : stringObjectMap.keySet()) {
                resultMap.put(key, stringObjectMap.get(key));
            }
        };

        Assertions.assertFalse(input.getIsStopped());

        input.start(consumer);
        input.stop();

        Assertions.assertTrue(input.getIsStopped());
    }

}