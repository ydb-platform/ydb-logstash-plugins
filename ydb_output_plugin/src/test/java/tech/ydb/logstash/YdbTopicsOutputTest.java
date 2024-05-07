package tech.ydb.logstash;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import co.elastic.logstash.api.*;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbTopicsOutputTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static String connectionString() {
        return ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();
    }

    private YdbTopicsOutput output;

    @BeforeEach
    public void setUp() {
        String connectionString = connectionString();
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(YdbTopicsOutput.PREFIX_CONFIG.name(), "message");
        configValues.put("topic_path", "fake_topic_path");
        configValues.put("connection_string", connectionString);
        configValues.put("consumer_name", "consumer");
        configValues.put("schema", "JSON");
        Configuration config = new ConfigurationImpl(configValues);

        output = new YdbTopicsOutput("test-output", config, null);
    }

    @Test
    public void testMessageWritingJson()  {
        CustomEvent event1 = new CustomEvent();
        event1.setField("key1", "value 1 2 3 4");

        output.output(List.of(event1));
        output.stop();

        Assertions.assertEquals("{\"key1\":\"value 1 2 3 4\"}", output.getCurrentMessage());
    }

    @Test
    public void testMessageWritingJsonTwoObjects()  {
        CustomEvent event1 = new CustomEvent();
        event1.setField("key1", "value 1 2 3 4");
        event1.setField("key2", null);

        output.output(List.of(event1));
        output.stop();

        Assertions.assertEquals("{\"key1\":\"value 1 2 3 4\",\"key2\":null}", output.getCurrentMessage());
    }
}
