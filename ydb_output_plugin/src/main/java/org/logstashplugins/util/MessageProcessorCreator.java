package org.logstashplugins.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import java.util.Map;

public class MessageProcessorCreator {
    private static final Logger logger = LoggerFactory.getLogger(MessageProcessorCreator.class);

    public static byte[] processJsonString(Map<String, Object> data) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(objectMapper.writeValueAsString(data));
            return objectMapper.writeValueAsBytes(jsonNode);
        } catch (IOException e) {
            logger.error("Error parsing JSON: {}", e.getMessage());
        }
        return null;
    }
}
