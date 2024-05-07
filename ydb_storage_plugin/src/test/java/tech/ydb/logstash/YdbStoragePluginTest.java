package tech.ydb.logstash;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.logstash.Event;
import org.logstash.plugins.ConfigurationImpl;

import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.impl.SimpleTableClient;
import tech.ydb.table.query.DataQueryResult;
import tech.ydb.table.result.ResultSetReader;
import tech.ydb.table.rpc.grpc.GrpcTableRpc;
import tech.ydb.table.transaction.TxControl;
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

    private static ResultSetReader executeQuery(String selectQuery) {
        DataQueryResult result = retryCtx.supplyResult(
                session -> session.executeDataQuery(selectQuery, TxControl.snapshotRo())
        ).join().getValue();
        return result.getResultSet(0);
    }

    private static Map<String, Object> createConfigMap() {
        Map<String, Object> config = new HashMap<>();
        String connectionString = ydb.useTls() ? "grpcs://" : "grpc://" + ydb.endpoint() + ydb.database();

        config.put(YdbStoragePlugin.CONNECTION.name(), connectionString);
        if (ydb.authToken() != null) {
            config.put(YdbStoragePlugin.TOKEN_AUTH.name(), ydb.authToken());
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
        config.put(YdbStoragePlugin.TABLE_NAME.name(), "logstash_simple_test");
        config.put(YdbStoragePlugin.TIMESTAMP_COLUMN_NAME.name(), "ts");

        try {
            YdbStoragePlugin plugin = new YdbStoragePlugin("test-simple", new ConfigurationImpl(config), null);

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

            ResultSetReader rs = executeQuery("SELECT * FROM logstash_simple_test ORDER by ts");
            Assertions.assertEquals(3, rs.getRowCount());

            Assertions.assertTrue(rs.next());

            Assertions.assertNotNull(rs.getColumn("id").getText());
            Assertions.assertEquals(TS1, rs.getColumn("ts").getTimestamp());
            Assertions.assertEquals("dev1", rs.getColumn("device").getText());
            Assertions.assertEquals(1.5d, rs.getColumn("value").getDouble());
            Assertions.assertEquals(1, rs.getColumn("priority").getUint16());

            Assertions.assertTrue(rs.next());
            Assertions.assertNotNull(rs.getColumn("id").getText());
            Assertions.assertEquals(TS2, rs.getColumn("ts").getTimestamp());
            Assertions.assertEquals("dev2", rs.getColumn("device").getText());
            Assertions.assertFalse(rs.getColumn("value").isOptionalItemPresent());
            Assertions.assertEquals(0x10000-3, rs.getColumn("priority").getUint16());

            Assertions.assertTrue(rs.next());
            Assertions.assertNotNull(rs.getColumn("id").getText());
            Assertions.assertEquals(TS3, rs.getColumn("ts").getTimestamp());
            Assertions.assertEquals("dev3", rs.getColumn("device").getText());
            Assertions.assertEquals(-1d, rs.getColumn("value").getDouble());
            Assertions.assertEquals(2, rs.getColumn("priority").getUint16());

            Assertions.assertFalse(rs.next());
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
        config.put(YdbStoragePlugin.TABLE_NAME.name(), "logstash_notnull_test");
        config.put(YdbStoragePlugin.ID_COLUMN_NAME.name(), "uuid");
        config.put(YdbStoragePlugin.TIMESTAMP_COLUMN_NAME.name(), "ts");

        try {
            YdbStoragePlugin plugin = new YdbStoragePlugin("test-notnull", new ConfigurationImpl(config), null);

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
            ev3.setField("priority", -2);

            plugin.output(Arrays.asList(ev1, ev3, ev2));

            ResultSetReader rs = executeQuery("SELECT * FROM logstash_notnull_test ORDER by ts");
            Assertions.assertEquals(2, rs.getRowCount()); // ev2 skipped because doens't have value

            Assertions.assertTrue(rs.next());

            Assertions.assertNotNull(rs.getColumn("uuid").getText());
            Assertions.assertEquals(TS1, rs.getColumn("ts").getTimestamp());
            Assertions.assertArrayEquals("dev1".getBytes(), rs.getColumn("device").getBytes());
            Assertions.assertEquals(1.5f, rs.getColumn("value").getFloat());
            Assertions.assertEquals(1, rs.getColumn("priority").getUint8());

            Assertions.assertTrue(rs.next());
            Assertions.assertNotNull(rs.getColumn("uuid").getText());
            Assertions.assertEquals(TS3, rs.getColumn("ts").getTimestamp());
            Assertions.assertArrayEquals("dev3".getBytes(), rs.getColumn("device").getBytes());
            Assertions.assertEquals(-1f, rs.getColumn("value").getFloat());
            Assertions.assertEquals(0x100-2, rs.getColumn("priority").getUint8());

            Assertions.assertFalse(rs.next());
        } finally {
            executeSchemeQuery("DROP TABLE logstash_notnull_test");
        }
   }
}