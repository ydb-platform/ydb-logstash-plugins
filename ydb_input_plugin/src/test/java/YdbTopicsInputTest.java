import co.elastic.logstash.api.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.logstash.plugins.ConfigurationImpl;
import org.logstashplugins.YdbTopicsInput;
import org.mockito.MockitoAnnotations;

import java.util.*;
import java.util.function.Consumer;

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class YdbTopicsInputTest {

    private YdbTopicsInput input;

    @Before
    public void setUp() {

        Map<String, Object> configValues = new HashMap<>();
        configValues.put(YdbTopicsInput.PREFIX_CONFIG.name(), "message");
        configValues.put(YdbTopicsInput.EVENT_COUNT_CONFIG.name(), 1L);
        configValues.put("topic_path", "fake_topic_path");
        configValues.put("connection_string", "grpc://localhost:2136?database=/local");
        configValues.put("consumer_name", "consumer");
        configValues.put("schema", "JSON");

        Configuration config = new ConfigurationImpl(configValues);

        input = new YdbTopicsInput("test-input", config, null);
        MockitoAnnotations.openMocks(this);

    }

    @After
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

        assertFalse(input.getIsStopped());

        input.start(consumer);
        input.stop();

        assertTrue(input.getIsStopped());
    }

}