package org.logstashplugins;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;

import org.logstashplugins.util.MessageHandler;
import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.read.AsyncReader;
import tech.ydb.topic.settings.ReadEventHandlersSettings;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @author Mikhail Lukashev
 */
@LogstashPlugin(name = "ydb_topics_input")
public class YdbTopicsInput implements Input {

    public static final PluginConfigSpec<Long> EVENT_COUNT_CONFIG =
            PluginConfigSpec.numSetting("count", 3);

    public static final PluginConfigSpec<String> PREFIX_CONFIG =
            PluginConfigSpec.stringSetting("prefix", "message");

    private final String topicPath;
    private final String connectionString;
    private final String id;
    private final String consumerName;
    private final String schema;

    private TopicClient topicClient;
    private AsyncReader reader;
    private GrpcTransport transport;
    private AuthProvider authProvider = NopAuthProvider.INSTANCE;
    private volatile boolean stopped = false;

    public YdbTopicsInput(String id, Configuration config, Context context) {
        this.id = id;
        topicPath = config.get(PluginConfigSpec.stringSetting("topic_path"));
        connectionString = config.get(PluginConfigSpec.stringSetting("connection_string"));
        consumerName = config.get(PluginConfigSpec.stringSetting("consumer_name"));
        schema = config.get(PluginConfigSpec.stringSetting("schema"));

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
    public void start(Consumer<Map<String, Object>> consumer) {
        initialize();

        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName(consumerName)
                .addTopic(TopicReadSettings.newBuilder()
                        .setPath(topicPath)
                        .setReadFrom(Instant.now().minus(Duration.ofHours(24)))
                        .setMaxLag(Duration.ofMinutes(30))
                        .build())
                .build();

        ReadEventHandlersSettings handlerSettings = ReadEventHandlersSettings.newBuilder()
                .setEventHandler(new MessageHandler(consumer, schema))
                .build();

        reader = topicClient.createAsyncReader(settings, handlerSettings);
        reader.init();
    }

    private void initialize() {
        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
        topicClient = TopicClient.newClient(transport).build();
    }

    @Override
    public void stop() {
        reader.shutdown();
        closeTransport();
        stopped = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {

    }

    private void closeTransport() {
        transport.close();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(EVENT_COUNT_CONFIG, PREFIX_CONFIG);
    }

    @Override
    public String getId() {
        return this.id;
    }

    public boolean getIsStopped() {
        return stopped;
    }
}