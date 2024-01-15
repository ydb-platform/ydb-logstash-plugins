package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;

import org.logstashplugins.util.MessageProcessor;
import org.logstashplugins.util.MessageProcessorCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.QueueOverflowException;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;


/**
 * @author Mikhail Lukashev
 */
@LogstashPlugin(name = "ydb_topics_output")
public class YdbTopicsOutput implements Output {

    public static final PluginConfigSpec<String> PREFIX_CONFIG = PluginConfigSpec.stringSetting("prefix", "");
    private final MessageProcessor messageProcessor = MessageProcessorCreator::processJsonString;
    private final Logger logger = LoggerFactory.getLogger(YdbTopicsOutput.class);
    private AuthProvider authProvider = NopAuthProvider.INSTANCE;
    private volatile boolean stopped = false;
    private final String topicPath;
    private final String producerId;
    private final String connectionString;
    private final String id;
    private String currentMessage;
    private GrpcTransport transport;
    private TopicClient topicClient;
    private AsyncWriter asyncWriter;

    public YdbTopicsOutput(String id, Configuration config, Context context) {
        this.id = id;
        topicPath = config.get(PluginConfigSpec.stringSetting("topic_path"));
        connectionString = config.get(PluginConfigSpec.stringSetting("connection_string"));
        producerId = config.get(PluginConfigSpec.stringSetting("producer_id"));

        String accessToken = config.get(PluginConfigSpec.stringSetting("access_token"));
        if (accessToken != null) {
            authProvider = new TokenAuthProvider(accessToken);
        } else {
            String saKeyFile = config.get(PluginConfigSpec.stringSetting("service_account_key"));
            if (saKeyFile != null) {
                authProvider = CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);
            }
        }
    }

    @Override
    public void output(Collection<Event> events) {
        initializeTransport();
        initializeWriter();
        Iterator<Event> z = events.iterator();
        while (z.hasNext() && !stopped) {
            byte[] message = messageProcessor.process(z.next().getData());
            sendMessage(message);
        }
    }

    private void initializeTransport() {
        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
        topicClient = TopicClient.newClient(transport).build();
    }

    private void initializeWriter() {
        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(topicPath)
                .setProducerId(producerId)
                .setMessageGroupId(producerId)
                .build();
        asyncWriter = topicClient.createAsyncWriter(settings);
        asyncWriter.init();
    }

    private void sendMessage(byte[] message) {
        try {
            asyncWriter.send(Message.of(message));
            currentMessage = new String(message);
        } catch (QueueOverflowException e) {
            logger.error("Error sending message to YDB Topics: " + e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        asyncWriter.shutdown();
        transport.close();
        stopped = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {

    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Collections.singletonList(PREFIX_CONFIG);
    }

    @Override
    public String getId() {
        return id;
    }

    public String getCurrentMessage() {
        return currentMessage;
    }
}
