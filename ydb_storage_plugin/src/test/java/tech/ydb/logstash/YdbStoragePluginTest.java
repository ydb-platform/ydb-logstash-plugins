package tech.ydb.logstash;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.Event;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.impl.SimpleTableClient;
import tech.ydb.table.query.Params;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.settings.ExecuteScanQuerySettings;
import tech.ydb.table.values.Value;
import tech.ydb.test.junit5.GrpcTransportExtension;
import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbStoragePluginTest {
    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    @RegisterExtension
    private final static GrpcTransportExtension transport = new GrpcTransportExtension();

    private final static SessionRetryContext retryCtx = SessionRetryContext.create(
            SimpleTableClient.newClient(GrpcTableRpc.useTransport(transport)).build()
    ).build();

    // some fixed timestamps for tests
    private static final Instant TS1 = Instant.ofEpochMilli(1581601437567L); // 2020-02-13 12:43:57.567 UTC
    private static final Instant TS2 = Instant.ofEpochMilli(1581601497120L); // 2020-02-13 12:44:57.120 UTC
    private static final Instant TS3 = Instant.ofEpochMilli(1581601498341L); // 2020-02-13 12:44:58.341 UTC

    private static void executeSchemeQuery(String sql) {
        retryCtx.supplyStatus(
                session -> session.executeSchemeQuery(sql)
        ).join().expectSuccess("cannot execute scheme query " + sql);
    }

    private static List<Map<String, Value<?>>> executeScanQuery(String sql) {
        List<Map<String, Value<?>>> rows = new ArrayList<>();

        retryCtx.supplyStatus(session -> {
            rows.clear();
            return session.executeScanQuery(sql, Params.empty(), ExecuteScanQuerySettings.newBuilder().build())
                    .start(rsr -> {
                        while (rsr.next()) {
                            Map<String, Value<?>> row = new HashMap<>();
                            for (int idx = 0; idx < rsr.getColumnCount(); idx++) {
                                row.put(rsr.getColumnName(idx), rsr.getColumn(idx).getValue());
                            }
                            rows.add(row);
                        }
                    });
        }).join().expectSuccess("cannot execute scan query " + sql);
        return rows;
    }

    private static Map<String, Object> createConfigMap() {
        Map<String, Object> config = new HashMap<>();
        String connectionString = ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();

        config.put(YdbStorage.CONNECTION.name(), connectionString);
        if (ydb.authToken() != null) {
            config.put(YdbStorage.TOKEN_AUTH.name(), ydb.authToken());
        }

        return config;
    }

    @Test
    public void testSimpleTable() {
        executeSchemeQuery(""
                + "CREATE TABLE logstash_simple_test("
                + "  id Text,"
                + "  ts Timestamp,"
                + "  device Text,"
                + "  value Double,"
                + "  priority Uint16,"
                + "  PRIMARY KEY (id)"
                + ");"
        );

        Map<String, Object> config = createConfigMap();
        config.put(YdbStorage.TABLE_NAME.name(), "logstash_simple_test");
        config.put(YdbStorage.UUID_COLUMN_NAME.name(), "id");
        config.put(YdbStorage.TIMESTAMP_COLUMN_NAME.name(), "ts");

        try {
            YdbStorage plugin = new YdbStorage("test-simple", new ConfigurationImpl(config), null);

            Event ev1 = new org.logstash.Event();
            ev1.setEventTimestamp(TS1);
            ev1.setField("device", "dev1");
            ev1.setField("value", 1.5d);
            ev1.setField("priority", 1);

            Event ev2 = new org.logstash.Event();
            ev2.setEventTimestamp(TS2);
            ev2.setField("device", "dev2");
            ev2.setField("priority", -3);

            Event ev3 = new org.logstash.Event();
            ev3.setEventTimestamp(TS3);
            ev3.setField("device", "dev3");
            ev3.setField("value", -1f);
            ev3.setField("priority", 2);

            plugin.output(Arrays.asList(ev1, ev3, ev2));

            List<Map<String, Value<?>>> rows = executeScanQuery("SELECT * FROM logstash_simple_test ORDER by ts");
            Assertions.assertEquals(3, rows.size());

            Assertions.assertTrue(rows.get(0).containsKey("id"));
            Assertions.assertEquals(TS1, rows.get(0).get("ts").asOptional().get().asData().getTimestamp());
            Assertions.assertEquals("dev1", rows.get(0).get("device").asOptional().get().asData().getText());
            Assertions.assertEquals(1.5d, rows.get(0).get("value").asOptional().get().asData().getDouble());
            Assertions.assertEquals(1, rows.get(0).get("priority").asOptional().get().asData().getUint16());

            Assertions.assertTrue(rows.get(1).containsKey("id"));
            Assertions.assertEquals(TS2, rows.get(1).get("ts").asOptional().get().asData().getTimestamp());
            Assertions.assertEquals("dev2", rows.get(1).get("device").asOptional().get().asData().getText());
            Assertions.assertFalse(rows.get(1).get("value").asOptional().isPresent());
            Assertions.assertEquals(0x10000-3, rows.get(1).get("priority").asOptional().get().asData().getUint16());

            Assertions.assertTrue(rows.get(2).containsKey("id"));
            Assertions.assertEquals(TS3, rows.get(2).get("ts").asOptional().get().asData().getTimestamp());
            Assertions.assertEquals("dev3", rows.get(2).get("device").asOptional().get().asData().getText());
            Assertions.assertEquals(-1d, rows.get(2).get("value").asOptional().get().asData().getDouble());
            Assertions.assertEquals(2, rows.get(2).get("priority").asOptional().get().asData().getUint16());
        } finally {
            executeSchemeQuery("DROP TABLE logstash_simple_test");
        }
    }

    @Test
    public void testNotNullTable() {
        executeSchemeQuery(""
                + "CREATE TABLE logstash_notnull_test("
                + "  uuid Text NOT NULL,"
                + "  ts Timestamp NOT NULL,"
                + "  device Bytes NOT NULL,"
                + "  value Float NOT NULL,"
                + "  priority Uint8 NOT NULL,"
                + "  PRIMARY KEY (uuid)"
                + ");"
        );

        Map<String, Object> config = createConfigMap();
        config.put(YdbStorage.TABLE_NAME.name(), "logstash_notnull_test");
        config.put(YdbStorage.TIMESTAMP_COLUMN_NAME.name(), "ts");

        try {
            YdbStorage plugin = new YdbStorage("test-notnull", new ConfigurationImpl(config), null);

            Event ev1 = new org.logstash.Event();
            ev1.setEventTimestamp(TS1);
            ev1.setField("uuid", "ev1");
            ev1.setField("device", "dev1");
            ev1.setField("value", 1.5d);
            ev1.setField("priority", 1);

            Event ev2 = new org.logstash.Event();
            ev2.setEventTimestamp(TS2);
            ev2.setField("uuid", "ev2");
            ev2.setField("device", "dev2");
            ev2.setField("priority", -3);

            Event ev3 = new org.logstash.Event();
            ev3.setEventTimestamp(TS3);
            ev3.setField("uuid", "ev3");
            ev3.setField("device", "dev3");
            ev3.setField("value", -1f);
            ev3.setField("priority", -2);

            plugin.output(Arrays.asList(ev1, ev3, ev2));

            List<Map<String, Value<?>>> rows = executeScanQuery("SELECT * FROM logstash_notnull_test ORDER by ts");
            Assertions.assertEquals(2, rows.size()); // ev2 skipped because doens't have value

            Assertions.assertEquals("ev1", rows.get(0).get("uuid").asData().getText());
            Assertions.assertEquals(TS1, rows.get(0).get("ts").asData().getTimestamp());
            Assertions.assertArrayEquals("dev1".getBytes(), rows.get(0).get("device").asData().getBytes());
            Assertions.assertEquals(1.5f, rows.get(0).get("value").asData().getFloat());
            Assertions.assertEquals(1, rows.get(0).get("priority").asData().getUint8());

            Assertions.assertEquals("ev3", rows.get(1).get("uuid").asData().getText());
            Assertions.assertEquals(TS3, rows.get(1).get("ts").asData().getTimestamp());
            Assertions.assertArrayEquals("dev3".getBytes(), rows.get(1).get("device").asData().getBytes());
            Assertions.assertEquals(-1f, rows.get(1).get("value").asData().getFloat());
            Assertions.assertEquals(0x100-2, rows.get(1).get("priority").asData().getUint8());
        } finally {
            executeSchemeQuery("DROP TABLE logstash_notnull_test");
        }
   }

   @Test
    public void testColumnTable() {
        executeSchemeQuery(""
                + "CREATE TABLE logstash_column_test("
                + "  id Text NOT NULL,"
                + "  ts Timestamp NOT NULL,"
                + "  device Text NOT NULL,"
                + "  value Double,"
                + "  priority Uint16,"
                + "  PRIMARY KEY (id)"
                + ") WITH (STORE = COLUMN);"
        );

        Map<String, Object> config = createConfigMap();
        config.put(YdbStorage.TABLE_NAME.name(), "logstash_column_test");
        config.put(YdbStorage.TIMESTAMP_COLUMN_NAME.name(), "ts");

        try {
            YdbStorage plugin = new YdbStorage("test-column", new ConfigurationImpl(config), null);

            Event ev1 = new org.logstash.Event();
            ev1.setEventTimestamp(TS1);
            ev1.setField("id", "e1");
            ev1.setField("device", "dev1");
            ev1.setField("value", 1.5d);
            ev1.setField("priority", 1);

            Event ev2 = new org.logstash.Event();
            ev2.setEventTimestamp(TS2);
            ev2.setField("id", "e2");
            ev2.setField("device", "dev2");
            ev2.setField("priority", -3);

            Event ev3 = new org.logstash.Event();
            ev3.setEventTimestamp(TS3);
            ev3.setField("id", "e3");
            ev3.setField("device", "dev3");
            ev3.setField("value", -1f);
            ev3.setField("priority", 2);

            Event ev4 = new org.logstash.Event();
            ev4.setEventTimestamp(TS2);
            ev4.setField("id", "e4");
            ev4.setField("value", -1f);
            ev4.setField("priority", 2);

            plugin.output(Arrays.asList(ev1, ev3, ev4, ev2));

            List<Map<String, Value<?>>> rows = executeScanQuery("SELECT * FROM logstash_column_test ORDER by ts");
            Assertions.assertEquals(3, rows.size());

            Assertions.assertEquals("e1", rows.get(0).get("id").asData().getText());
            Assertions.assertEquals(TS1, rows.get(0).get("ts").asData().getTimestamp());
            Assertions.assertEquals("dev1", rows.get(0).get("device").asData().getText());
            Assertions.assertEquals(1.5d, rows.get(0).get("value").asOptional().get().asData().getDouble());
            Assertions.assertEquals(1, rows.get(0).get("priority").asOptional().get().asData().getUint16());

            Assertions.assertEquals("e2", rows.get(1).get("id").asData().getText());
            Assertions.assertEquals(TS2, rows.get(1).get("ts").asData().getTimestamp());
            Assertions.assertEquals("dev2", rows.get(1).get("device").asData().getText());
            Assertions.assertFalse(rows.get(1).get("value").asOptional().isPresent());
            Assertions.assertEquals(0x10000-3, rows.get(1).get("priority").asOptional().get().asData().getUint16());

            Assertions.assertEquals("e3", rows.get(2).get("id").asData().getText());
            Assertions.assertEquals(TS3, rows.get(2).get("ts").asData().getTimestamp());
            Assertions.assertEquals("dev3", rows.get(2).get("device").asData().getText());
            Assertions.assertEquals(-1d, rows.get(2).get("value").asOptional().get().asData().getDouble());
            Assertions.assertEquals(2, rows.get(2).get("priority").asOptional().get().asData().getUint16());
        } finally {
            executeSchemeQuery("DROP TABLE logstash_column_test");
        }
    }
}