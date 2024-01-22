package org.logstashplugins;

import java.io.OutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import co.elastic.logstash.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.auth.AuthProvider;
import tech.ydb.auth.NopAuthProvider;
import tech.ydb.auth.TokenAuthProvider;
import tech.ydb.auth.iam.CloudAuthHelper;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.*;

// class name must match plugin name
@LogstashPlugin(name = "ydb_storage_plugin")
public class YdbStoragePlugin implements Output {

    private static final Logger log = LoggerFactory.getLogger(YdbStoragePlugin.class);

    public static final PluginConfigSpec<String> CONNECTION_STRING =
            PluginConfigSpec.stringSetting("connection_string", "");

    public static final PluginConfigSpec<String> SA_KEY_FILE =
            PluginConfigSpec.stringSetting("sa_key_file", "not", false, false);

    public static final PluginConfigSpec<String> TOKEN_AUTH =
            PluginConfigSpec.stringSetting("token_auth", "not", false, false);

    public static final PluginConfigSpec<String> TABLE_NAME =
            PluginConfigSpec.stringSetting("table_name", "logstash", false, true);

    public static final PluginConfigSpec<Boolean> CREATE_TABLE =
            PluginConfigSpec.booleanSetting("create_table", false, false, false);

    public static final PluginConfigSpec<String> COLUMNS =
            PluginConfigSpec.stringSetting("columns");

    public static final PluginConfigSpec<String> NAME_IDENTIFIER_COLUMN =
            PluginConfigSpec.stringSetting("name_identifier_column", "id", false, false);


    private final String id;
    private PrintStream printer;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped = false;
    private GrpcTransport transport;
    private TableClient tableClient;
    private String database;
    private SessionRetryContext retryCtx;
    private String tableName;
    private String connectionString;
    private String saKeyFile;
    private String accessToken;
    private String columnsInConfig;
    public StructType messageType;
    private String tablePath;
    private Boolean createTable;
    private HashMap<String, String> columns;
    private Map<String, PrimitiveType> primitiveType;
    private String idName;

    // all plugins must provide a constructor that accepts id, Configuration, and Context
    public YdbStoragePlugin(final String id, final Configuration configuration, final Context context) {
        this(id, configuration, context, System.out);
    }

    YdbStoragePlugin(final String id, final Configuration config, final Context context, OutputStream targetStream) {
        log.info("init YDBStoragePlugin");

        // constructors should validate configuration options
        this.id = id;
        idName = config.get(NAME_IDENTIFIER_COLUMN);
        printer = new PrintStream(targetStream);
        connectionString = config.get(CONNECTION_STRING);
        tableName = config.get(TABLE_NAME);
        saKeyFile = config.get(SA_KEY_FILE);
        columnsInConfig = config.get(COLUMNS);
        createTable = config.get(CREATE_TABLE);
        accessToken = config.get(TOKEN_AUTH);

        try {
            createPrimitiveType();
            createColumns();
            createStructType();
            createSession();
            createTablePath();
        } catch (RuntimeException e) {
            log.error("Can't initialize YDBStoragePlugin", e);
            stopped = true;
        }
    }

    private void createPrimitiveType() {
        primitiveType = new HashMap<>();
        for (PrimitiveType primitiveType1 : PrimitiveType.values()) {
            primitiveType.put(primitiveType1.toString(), primitiveType1);
        }
    }

    private void createStructType() {
        Map<String, Type> members = new HashMap<>();
        members.put(idName, PrimitiveType.Text);
        for (String column : columns.keySet()) {
            members.put(column, primitiveType.get(columns.get(column)));
        }
        messageType = StructType.of(members);
    }

    private void createColumns() {
        String[] splitColumnsInConfig = columnsInConfig.split(",");
        columns = new HashMap<>();
        if (splitColumnsInConfig.length > 1){
            for (int i = 0; i < splitColumnsInConfig.length; i = i + 2) {
                columns.put(splitColumnsInConfig[i].trim(), splitColumnsInConfig[i + 1].trim());
            }
        }
    }

    private void createSession() {
        AuthProvider authProvider = createAuthProvider();
        transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(authProvider)
                .build();
        tableClient = TableClient
                .newClient(transport)
                .build();
        log.info("create session");
        this.database = transport.getDatabase();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    private AuthProvider createAuthProvider() {
        if (!saKeyFile.equals("not")) {
            return CloudAuthHelper.getServiceAccountFileAuthProvider(saKeyFile);
        }
        if (!accessToken.equals("not")) {
            return new TokenAuthProvider(accessToken);
        }

        return NopAuthProvider.INSTANCE;
    }

    private void createTablePath() {
        if (createTable) {
            makeTable();
        } else {
            tablePath = database + "/" + tableName;
        }
    }

    private void makeTable() {
        TableDescription.Builder logstashTableBuilder = TableDescription.newBuilder();
        logstashTableBuilder.addNonnullColumn(idName, PrimitiveType.Text);
        logstashTableBuilder.setPrimaryKey(idName);
        for (String column : columns.keySet()) {
            logstashTableBuilder.addNullableColumn(column, primitiveType.get(columns.get(column)));
        }
        TableDescription logstashTable = logstashTableBuilder.build();
        tablePath = database + "/" + tableName;

        log.info("create table {}", tablePath);
        retryCtx.supplyStatus(session -> session.createTable(tablePath, logstashTable))
                .join()
                .expectSuccess("Can't create table /" + tableName);
    }

    @Override
    public void output(final Collection<Event> events) {
        Iterator<Event> z = events.iterator();
        List<Map<String, Value<?>>> allEvents = new ArrayList<>();
        while (z.hasNext() && !stopped) {
            Event event = z.next();
            UUID uuid = UUID.randomUUID();
            Map<String, Value<?>> eventValue = new HashMap<>();
            String eventID = uuid.toString();
            eventValue.put(idName, PrimitiveValue.newText(eventID));
            log.debug("create event with id {} -> {}", eventID, event.getData());
            for (String columnName : columns.keySet()) {
                Object data = event.getField(columnName);
                eventValue.put(columnName, createColumnValue(columnName, data));
            }
            allEvents.add(eventValue);
            printer.println(event);
        }
        upsertInDb(allEvents);
    }

    private PrimitiveValue createColumnValue(String nameData, Object data) {
        log.debug("create PrimitiveValue with name {} and {}", nameData, data.getClass().getName());
        if (data instanceof Boolean) {
            return PrimitiveValue.newBool((Boolean) data);
        } else if (data instanceof Byte) {
            return PrimitiveValue.newInt8((Byte) data);
        } else if (data instanceof Short) {
            return PrimitiveValue.newInt16((Short) data);
        } else if (data instanceof Integer) {
            if (columns.get(nameData).equals("Uint16"))
                return PrimitiveValue.newUint16((Integer) data);
            else if (columns.get(nameData).equals("Int32"))
                return PrimitiveValue.newInt32((Integer) data);
        } else if (data instanceof Long) {
            Long value = (Long)data;
            switch (columns.get(nameData)) {
                case "Uint32":
                    return PrimitiveValue.newUint32(value);
                case "Int64":
                    return PrimitiveValue.newInt64(value);
                case "Uint64":
                    return PrimitiveValue.newUint64(value);
                case "Int32":
                    return PrimitiveValue.newInt32(value.intValue());
                case "Date":
                    return PrimitiveValue.newDate(value);
                case "Datetime":
                    return PrimitiveValue.newDatetime(value);
                case "Timestamp":
                    return PrimitiveValue.newTimestamp(value);
                default:
                    break;
            }
        } else if (data instanceof Float) {
            return PrimitiveValue.newFloat((Float) data);
        } else if (data instanceof Double) {
            return PrimitiveValue.newDouble((Double) data);
        } else if (data instanceof String) {
            return PrimitiveValue.newText((String) data);
        } else if (data instanceof LocalDateTime) {
            return PrimitiveValue.newDatetime((LocalDateTime) data);
        } else if (data instanceof LocalDate) {
            return PrimitiveValue.newDate((LocalDate) data);
        } else if (data instanceof Instant) {
            Instant instant = (Instant)data;
            switch (columns.get(nameData)) {
                case "Date":
                    return PrimitiveValue.newDate(instant);
                case "Datetime":
                    return PrimitiveValue.newDatetime(instant);
                case "Timestamp":
                    return PrimitiveValue.newTimestamp(instant);
                default:
                    break;
            }
        }
        return PrimitiveValue.newText(data.toString());
    }

    private void upsertInDb(List<Map<String, Value<?>>> allEvents) {
        if (allEvents.isEmpty()) {
            return;
        }
        ListValue messages = toListValue(allEvents);
        retryCtx.supplyStatus(session
                        -> session.executeBulkUpsert(tablePath, messages, new BulkUpsertSettings()))
                .join().expectSuccess("bulk upsert problem");
    }

    @Override
    public void stop() {
        tableClient.close();
        transport.close();
        stopped = true;
        done.countDown();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return new ArrayList<>(List.of(CONNECTION_STRING, SA_KEY_FILE, TABLE_NAME, COLUMNS, CREATE_TABLE,
                NAME_IDENTIFIER_COLUMN, TOKEN_AUTH));
    }

    @Override
    public String getId() {
        return id;
    }

    private ListValue toListValue(List<Map<String, Value<?>>> allEvents) {
        ListType listType = ListType.of(messageType);
        return listType.newValue(allEvents.stream()
                .map(e -> messageType.newValue(e))
                .collect(Collectors.toList()));
    }

}