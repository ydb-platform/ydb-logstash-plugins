package org.logstashplugins.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class MessageHandler extends AbstractReadEventHandler {
    private final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final Consumer<Map<String, Object>> consumer;
    private final MessageProcessor messageProcessor;

    public MessageHandler(Consumer<Map<String, Object>> consumer, String schema) {
        this.consumer = consumer;

        if (schema.equals("JSON")) {
            messageProcessor = this::processJsonMessage;
        } else {
            messageProcessor = this::processNonJsonMessage;
        }
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        for (Message message : event.getMessages()) {
            logger.info("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());
            Map<String, Object> logstashEvent = messageProcessor.process(message, consumer);
            consumer.accept(logstashEvent);
            message.commit().join();
        }
    }

    private Map<String, Object> processJsonMessage(Message message, Consumer<Map<String, Object>> consumer) {
        Map<String, Object> logstashEvent = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(new String(message.getData()));
            parseJson(jsonNode, logstashEvent, objectMapper);
        } catch (IOException e) {
            logger.error("Error parsing JSON: {}", e.getMessage());
        }
        return logstashEvent;
    }

    private Map<String, Object> processNonJsonMessage(Message message, Consumer<Map<String, Object>> consumer) {
        return Collections.singletonMap("message", new String(message.getData()));
    }

    private void parseJson(JsonNode jsonNode, Map<String, Object> result, ObjectMapper objectMapper) {
        if (jsonNode.isObject()) {
            Map<String, Object> nestedMap = new LinkedHashMap<>();
            jsonNode.fields().forEachRemaining(entry -> {
                if (entry.getValue().isObject() || entry.getValue().isArray()) {
                    try {
                        String jsonStr = objectMapper.writeValueAsString(entry.getValue());
                        nestedMap.put(entry.getKey(), jsonStr);
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                } else {
                    nestedMap.put(entry.getKey(), entry.getValue().asText());
                }
            });
            result.putAll(nestedMap);
        }
    }
}
