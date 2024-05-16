package tech.ydb.logstash;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.PluginConfigSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.topic.TopicClient;
import tech.ydb.topic.settings.WriterSettings;
import tech.ydb.topic.write.AsyncWriter;
import tech.ydb.topic.write.Message;
import tech.ydb.topic.write.QueueOverflowException;


/**
 * @author Mikhail Lukashev
 */
@LogstashPlugin(name = "ydb_topics_output")
public class YdbTopicsOutput implements Output {
    private final Logger logger = LoggerFactory.getLogger(YdbTopicsOutput.class);

    static final PluginConfigSpec<String> CONNECTION = PluginConfigSpec.requiredStringSetting("connection_string");
    static final PluginConfigSpec<String> SA_KEY_FILE = PluginConfigSpec.stringSetting("sa_key_file");
    static final PluginConfigSpec<String> TOKEN_AUTH = PluginConfigSpec.stringSetting("token_auth");
    static final PluginConfigSpec<String> TOKEN_FILE = PluginConfigSpec.stringSetting("token_file");
    static final PluginConfigSpec<Boolean> USE_METADATA = PluginConfigSpec.booleanSetting("use_metadata");

    static final PluginConfigSpec<String> TOPIC_PATH = PluginConfigSpec.requiredStringSetting("topic_path");

    private final Function<Event, Message> convertor = MessageProcessor::processJsonString;
    private final String id;
    private final GrpcTransport transport;
    private final TopicClient topicClient;
    private final AsyncWriter asyncWriter;
    private final CountDownLatch stopped = new CountDownLatch(1);

    public YdbTopicsOutput(String id, Configuration config, Context context) {
        this.id = id;
        String topicPath = config.get(TOPIC_PATH);
        String connectionString = config.get(CONNECTION);

        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(createAuthProvider(config))
                .build();
        topicClient = TopicClient.newClient(transport).build();

        WriterSettings settings = WriterSettings.newBuilder()
                .setTopicPath(topicPath)
                .build();
        asyncWriter = topicClient.createAsyncWriter(settings);
        asyncWriter.init();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void output(Collection<Event> events) {
        try {
            for (Event event : events) {
                asyncWriter.send(convertor.apply(event));
            }
        } catch (QueueOverflowException e) {
            logger.error("Error sending message to YDB Topics: " + e.getMessage(), e);
        }
    }

    @Override
    public void stop() {
        asyncWriter.shutdown().thenRun(() -> {
            topicClient.close();
            transport.close();
            stopped.countDown();
        });
    }

    @Override
    public void awaitStop() throws InterruptedException {
        stopped.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(CONNECTION,
                SA_KEY_FILE,
                TOKEN_AUTH,
                TOKEN_FILE,
                USE_METADATA,

                TOPIC_PATH
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
