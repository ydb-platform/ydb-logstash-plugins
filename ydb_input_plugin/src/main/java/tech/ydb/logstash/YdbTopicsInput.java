package tech.ydb.logstash;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;

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

/**
 * @author Mikhail Lukashev
 */
@LogstashPlugin(name = "ydb_topics_input")
public class YdbTopicsInput implements Input {
    static final PluginConfigSpec<String> CONNECTION = PluginConfigSpec.requiredStringSetting("connection_string");
    static final PluginConfigSpec<String> SA_KEY_FILE = PluginConfigSpec.stringSetting("sa_key_file");
    static final PluginConfigSpec<String> TOKEN_AUTH = PluginConfigSpec.stringSetting("token_auth");
    static final PluginConfigSpec<String> TOKEN_FILE = PluginConfigSpec.stringSetting("token_file");
    static final PluginConfigSpec<Boolean> USE_METADATA = PluginConfigSpec.booleanSetting("use_metadata");

    static final PluginConfigSpec<String> TOPIC_PATH = PluginConfigSpec.stringSetting("topic_path");
    static final PluginConfigSpec<String> CONSUMER_NAME = PluginConfigSpec.stringSetting("consumer_name");
    static final PluginConfigSpec<String> SCHEMA = PluginConfigSpec.stringSetting("schema");

    private final String topicPath;
    private final String connectionString;
    private final String id;
    private final String consumerName;
    private final String schema;

    private TopicClient topicClient;
    private AsyncReader reader;
    private GrpcTransport transport;
    private AuthProvider authProvider = NopAuthProvider.INSTANCE;

    public YdbTopicsInput(String id, Configuration config, Context context) {
        this.id = id;

        this.connectionString = config.get(CONNECTION);
        this.authProvider = createAuthProvider(config);

        this.topicPath = config.get(TOPIC_PATH);
        this.consumerName = config.get(CONSUMER_NAME);
        this.schema = config.get(SCHEMA);
    }

    @Override
    public String getId() {
        return this.id;
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
        topicClient = TopicClient.newClient(transport).build();

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

    @Override
    public void stop() {
        reader.shutdown();
        transport.close();
    }

    @Override
    public void awaitStop() throws InterruptedException {

    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(CONNECTION,
                SA_KEY_FILE,
                TOKEN_AUTH,
                TOKEN_FILE,
                USE_METADATA,

                TOPIC_PATH,
                CONSUMER_NAME,
                SCHEMA
        );
    }

    private static AuthProvider createAuthProvider(Configuration config) {
        String saKeyFile = config.get(SA_KEY_FILE);
        if (saKeyFile != null && !saKeyFile.isEmpty()) {
            return CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);
        }
        String tokenAuth = config.get(TOKEN_AUTH);
        if (tokenAuth != null && !tokenAuth.isEmpty()) {
            return new TokenAuthProvider(tokenAuth);
        }

        String tokenFile = config.get(TOKEN_FILE);
        if (tokenFile != null && !tokenFile.isEmpty()) {
            try {
                Optional<String> token = Files.lines(Paths.get(tokenFile)).findFirst();
                if (token.isPresent()) {
                    return new TokenAuthProvider(token.get());
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot read token from " + tokenFile, e);
            }
        }

        Boolean useMetadata = config.get(USE_METADATA);
        if (useMetadata != null && useMetadata) {
            return CloudAuthHelper.getMetadataAuthProvider();
        }

        return NopAuthProvider.INSTANCE;
    }
}