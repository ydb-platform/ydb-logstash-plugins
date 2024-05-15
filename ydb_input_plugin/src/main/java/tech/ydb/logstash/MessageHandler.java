package tech.ydb.logstash;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.AbstractReadEventHandler;
import tech.ydb.topic.read.events.DataReceivedEvent;

import static com.fasterxml.jackson.databind.node.JsonNodeType.NULL;
import static com.fasterxml.jackson.databind.node.JsonNodeType.POJO;
import static com.fasterxml.jackson.databind.node.JsonNodeType.STRING;

public class MessageHandler extends AbstractReadEventHandler {
    private final Logger logger = LoggerFactory.getLogger(MessageHandler.class);
    private final Consumer<Map<String, Object>> consumer;
    private final Function<Message, Map<String, Object>> messageProcessor;

    public MessageHandler(Consumer<Map<String, Object>> consumer, String schema) {
        this.consumer = consumer;

        if ("JSON".equals(schema)) {
            messageProcessor = this::processJsonMessage;
        } else {
            messageProcessor = this::processNonJsonMessage;
        }
    }

    @Override
    public void onMessages(DataReceivedEvent event) {
        for (Message message : event.getMessages()) {
            logger.debug("Message received. SeqNo={}, offset={}", message.getSeqNo(), message.getOffset());
            consumer.accept(messageProcessor.apply(message));
            message.commit().join();
        }
    }

    private Map<String, Object> processJsonMessage(Message message) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = parseJson(mapper, message.getData());
            if (map == null) {
                return processNonJsonMessage(message);
            }
            return map;
        } catch (IOException e) {
            logger.error("Error parsing JSON: {}", e.getMessage());
            return processNonJsonMessage(message);
        }
    }

    private Map<String, Object> processNonJsonMessage(Message message) {
        return Collections.singletonMap("message", message.getData());
    }

    private Map<String, Object> parseJson(ObjectMapper mapper, byte[] json) throws IOException {
        JsonNode jsonNode = mapper.readTree(json);
        if (!jsonNode.isObject()) {
            return null;
        }

        Map<String, Object> map = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> iter = jsonNode.fields();
        while (iter.hasNext()) {
            Map.Entry<String, JsonNode> ev = iter.next();
            String name = ev.getKey();
            JsonNode node = ev.getValue();
            switch (node.getNodeType()) {
                case STRING:
                    map.put(name, node.asText());
                    break;
                case BINARY:
                    map.put(name, node.binaryValue());
                    break;
                case BOOLEAN:
                    map.put(name, node.booleanValue());
                    break;
                case NUMBER:
                    if (node.isLong()) {
                        map.put(name, node.longValue());
                    } else if (node.isDouble()) {
                        map.put(name, node.doubleValue());
                    } else {
                        map.put(name, node.decimalValue());
                    }
                    break;
                case OBJECT:
                case ARRAY:
                case POJO:
                    map.put(name, mapper.writeValueAsString(node));
                    break;
                case NULL:
                case MISSING:
                default:
                    break;
            }
        }

        return map;
    }
}
