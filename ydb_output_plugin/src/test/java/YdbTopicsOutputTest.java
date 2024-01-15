import co.elastic.logstash.api.*;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstashplugins.YdbTopicsOutput;
import util.CustomEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

public class YdbTopicsOutputTest {
    private static final String CONNECTION_STRING = "grpc://localhost:2136?database=/local";
    private static final String TOPIC_PATH = "my-topic";

    @Test
    public void testMessageWritingJson()  {
        Map<String, Object> configValues = new HashMap<>();
        configValues.put("topic_path", TOPIC_PATH);
        configValues.put("connection_string", CONNECTION_STRING);
        configValues.put("schema", "JSON");
        Configuration config = new ConfigurationImpl(configValues);
        YdbTopicsOutput output = new YdbTopicsOutput("test", config, null);
        CustomEvent event1 = new CustomEvent();
        event1.setField("key1", "value 1 2 3 4");

        output.output(List.of(event1));
        output.stop();

        assertEquals(output.getCurrentMessage(), "{\"key1\":\"value 1 2 3 4\"}");
    }

    @Test
    public void testMessageWritingJsonTwoObjects()  {
        Map<String, Object> configValues = new HashMap<>();
        configValues.put("topic_path", TOPIC_PATH);
        configValues.put("connection_string", CONNECTION_STRING);
        configValues.put("schema", "JSON");
        Configuration config = new ConfigurationImpl(configValues);
        YdbTopicsOutput output = new YdbTopicsOutput("test", config, null);
        CustomEvent event1 = new CustomEvent();
        event1.setField("key1", "value 1 2 3 4");
        event1.setField("key2", null);

        output.output(List.of(event1));
        output.stop();

        assertEquals(output.getCurrentMessage(), "{\"key1\":\"value 1 2 3 4\",\"key2\":null}");
    }
}
