package org.logstashplugins.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.read.impl.events.DataReceivedEventImpl;

import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;


public class MessageHandlerTest {
    @Mock
    private Consumer<Map<String, Object>> consumer;

    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testOnMessagesBinary() {
        String json = "{\n" +
                "    \"name\": \"user\",\n" +
                "    \"email\": \"user@user.com\"\n" +
                "}";
        MessageHandler messageHandler = new MessageHandler(consumer, "BINARY");
        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);
        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(Collections.singletonList(message),
                null, null);

        messageHandler.onMessages(dataReceivedEvent);

        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }

    @Test
    public void testOnMessageJson() {
        String json = " { \"name\": \"example\", \"meta\": { \"id\" : 1, \"level\" : 3 } }}";
        MessageHandler messageHandler = new MessageHandler(consumer, "JSON");
        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);
        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(Collections.singletonList(message),
                null, null);

        messageHandler.onMessages(dataReceivedEvent);

        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }
}
