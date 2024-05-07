package tech.ydb.logstash;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import co.elastic.logstash.api.*;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.table.description.TableColumn;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.*;

// class name must match plugin name
@LogstashPlugin(name = "ydb_storage_plugin")
public class YdbStoragePlugin implements Output {
    private interface FieldReader {
        Value<?> readField(Event event);
    }

    public static final PluginConfigSpec<String> CONNECTION = PluginConfigSpec.requiredStringSetting("connection_string");
    public static final PluginConfigSpec<String> SA_KEY_FILE = PluginConfigSpec.stringSetting("sa_key_file");
    public static final PluginConfigSpec<String> TOKEN_AUTH = PluginConfigSpec.stringSetting("token_auth");
    public static final PluginConfigSpec<String> TOKEN_FILE = PluginConfigSpec.stringSetting("token_file");
    public static final PluginConfigSpec<Boolean> USE_METADATA = PluginConfigSpec.booleanSetting("use_metadata");

    public static final PluginConfigSpec<String> TABLE_NAME = PluginConfigSpec.stringSetting("table_name", "logstash", false, true);
    public static final PluginConfigSpec<String> ID_COLUMN_NAME = PluginConfigSpec.stringSetting("column_id", "id");
    public static final PluginConfigSpec<String> TIMESTAMP_COLUMN_NAME = PluginConfigSpec.stringSetting("column_timestamp");
    public static final PluginConfigSpec<Map<String, Object>> COLUMNS = PluginConfigSpec.hashSetting("columns");

    private final String id;
    private final String tablePath;
//    private final Logger logger;

    private final StructType tableRowType;
    private final FieldReader[] fieldReaders;

    private final YdbClient client;
    private final CountDownLatch stopped = new CountDownLatch(1);

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public YdbStoragePlugin(String id, Configuration cfg, Context ctx) {
        this(id, cfg, ctx, YdbClientImpl::new, UUID::randomUUID);
    }

    YdbStoragePlugin(String id, Configuration cfg, Context ctx, YdbClient.Factory factory, Supplier<UUID> uuidSupplier) {
        this.id = id;
//        this.logger = context.getLogger();

        String connectionString = cfg.get(CONNECTION);
        AuthProvider auth = createAuthProvider(cfg);
        YdbClient ydbClient = factory.create(connectionString, auth);

        try {
            String tableName = cfg.get(TABLE_NAME);
            String columnId = cfg.get(ID_COLUMN_NAME);
            String columnTimestamp = cfg.get(TIMESTAMP_COLUMN_NAME);

            this.tablePath = tableName.startsWith("/") ? tableName : ydbClient.getDatabase() + "/" + tableName;

            TableDescription description = ydbClient.desribeTable(tablePath);

            this.tableRowType = StructType.of(
                    description.getColumns().stream().collect(
                            Collectors.toMap(TableColumn::getName, TableColumn::getType)
                    )
            );

            Set<String> columnNames = new HashSet<>();
            this.fieldReaders = new FieldReader[tableRowType.getMembersCount()];
            for (int idx = 0; idx < tableRowType.getMembersCount(); idx += 1) {
                String name = tableRowType.getMemberName(idx);
                Type type = tableRowType.getMemberType(idx);
                columnNames.add(name);
                if (columnId != null && columnId.equals(name)) {
                    fieldReaders[idx] = uuidFieldGenerator(type, uuidSupplier);
                } else if (columnTimestamp != null && columnTimestamp.equals(name)) {
                    fieldReaders[idx] = timestampFieldReader(type);
                } else {
                    fieldReaders[idx] = eventFieldReader(name, type);
                }
            }

            if (columnId != null && !columnNames.contains(columnId)) {
                throw new IllegalStateException("Table " + tableName + " doesn't have column " + columnId);
            }
            if (columnTimestamp != null && !columnNames.contains(columnTimestamp)) {
                throw new IllegalStateException("Table " + tableName + " doesn't have column " + columnTimestamp);
            }

            this.client = ydbClient;
        } catch (RuntimeException ex) {
            ydbClient.close();
            this.stopped.countDown();
            throw ex;
        }
    }

    @Override
    public void output(final Collection<Event> events) {
        if (events.isEmpty() || client == null) {
            return;
        }

        List<Value<?>> eventToWrite = events.stream().map(ev -> {
            Value<?>[] fields = new Value[fieldReaders.length];
            for (int idx = 0; idx < fieldReaders.length; idx++) {
                fields[idx] = fieldReaders[idx].readField(ev);
                if (fields[idx] == null) {
                    return null;
                }
            }
            return tableRowType.newValueUnsafe(fields);
        }).filter(Objects::nonNull).collect(Collectors.toList());
        client.bulkUpsert(tablePath, ListType.of(tableRowType).newValue(eventToWrite));
    }

    @Override
    public void stop() {
        client.close();
        stopped.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        stopped.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return Arrays.asList(
                CONNECTION,
                SA_KEY_FILE,
                TOKEN_AUTH,
                TOKEN_FILE,
                USE_METADATA,
                TABLE_NAME,
                ID_COLUMN_NAME,
                TIMESTAMP_COLUMN_NAME,
                COLUMNS
        );
    }

    @Override
    public String getId() {
        return id;
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

    private static FieldReader uuidFieldGenerator(Type type, Supplier<UUID> generator) {
        if (type instanceof PrimitiveType) {
            return ev -> TypeConverter.toValue(generator.get(), (PrimitiveType) type);
        }
        if (type instanceof OptionalType) {
            OptionalType optional = (OptionalType) type;
            if (optional.getItemType() instanceof PrimitiveType) {
                PrimitiveType inner = (PrimitiveType) optional.getItemType();
                return ev -> optional.newValue(TypeConverter.toValue(generator.get(), inner));
            }
        }
        throw new IllegalStateException("Unsupported type " + type + " for uuid column");
    }

    private static FieldReader timestampFieldReader(Type type) {
        if (type instanceof PrimitiveType) {
            return ev -> TypeConverter.toValue(ev.getEventTimestamp(), (PrimitiveType) type);
        }
        if (type instanceof OptionalType) {
            OptionalType optional = (OptionalType) type;
            if (optional.getItemType() instanceof PrimitiveType) {
                PrimitiveType inner = (PrimitiveType) optional.getItemType();
                return ev -> optional.newValue(TypeConverter.toValue(ev.getEventTimestamp(), inner));
            }
        }
        throw new IllegalStateException("Unsupported type " + type + " for timestamp column");
    }

    private static FieldReader eventFieldReader(String name, Type type) {
        if (type instanceof PrimitiveType) {
            return ev -> TypeConverter.toValue(ev.getField(name), (PrimitiveType) type);
        }
        if (type instanceof OptionalType) {
            OptionalType optional = (OptionalType) type;

            if (optional.getItemType() instanceof PrimitiveType) {
                PrimitiveType inner = (PrimitiveType) optional.getItemType();
                return (ev) -> {
                    Object value = ev.getField(name);
                    if (value == null) {
                        return optional.emptyValue();
                    } else {
                        return optional.newValue(TypeConverter.toValue(value, inner));
                    }
                };
            }
        }
        throw new IllegalStateException("Unsupported type " + type + " for field " + name);
    }
}
