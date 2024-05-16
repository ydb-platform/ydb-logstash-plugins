package tech.ydb.logstash;



import java.util.SortedMap;
import java.util.TreeMap;

import co.elastic.logstash.api.Event;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import tech.ydb.topic.write.Message;

public class MessageProcessor {
    public static Message processJsonString(Event event) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            SortedMap<String, Object> json = new TreeMap<>();
            json.put("timestamp", event.getEventTimestamp().toEpochMilli());

            event.getData().forEach((name, value) -> {
                if (value == null) {
                    return;
                }
                if ("org.jruby.RubyString".equals(value.getClass().getName())) {
                    json.put(name, value.toString());
                }
                if ("org.jruby.RubyFixnum".equals(value.getClass().getName())) {
                    json.put(name, Long.valueOf(value.toString()));
                }
                if ("org.jruby.RubyFloat".equals(value.getClass().getName())) {
                    json.put(name, Float.valueOf(value.toString()));
                }
            });
            return Message.of(mapper.writer().writeValueAsBytes(json));
        } catch (JsonProcessingException e) {
            return Message.of(e.getMessage().getBytes());
        }
    }
}
