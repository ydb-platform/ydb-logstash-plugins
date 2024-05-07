package tech.ydb.logstash;


import org.junit.jupiter.api.Test;



public class MessageHandlerTest {

    @Test
    public void testOnMessagesBinary() {
//        String json = "{\n" +
//                "    \"name\": \"user\",\n" +
//                "    \"email\": \"user@user.com\"\n" +
//                "}";
//        MessageHandler messageHandler = new MessageHandler(consumer, "BINARY");
//        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);
//        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(Collections.singletonList(message),
//                null, null);
//
//        messageHandler.onMessages(dataReceivedEvent);
//
//        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }

    @Test
    public void testOnMessageJson() {
//        String json = " { \"name\": \"example\", \"meta\": { \"id\" : 1, \"level\" : 3 } }}";
//        MessageHandler messageHandler = new MessageHandler(consumer, "JSON");
//        CustomMessage message = new CustomMessage(json.getBytes(), 0, 0);
//        DataReceivedEvent dataReceivedEvent = new DataReceivedEventImpl(
//                Collections.singletonList(message), null, null);
//
//        messageHandler.onMessages(dataReceivedEvent);
//
//        Mockito.verify(consumer, Mockito.times(1)).accept(Mockito.any());
    }
}
