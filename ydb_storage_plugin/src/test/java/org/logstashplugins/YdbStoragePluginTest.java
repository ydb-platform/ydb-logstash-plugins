package org.logstashplugins;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.*;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Event;
import org.logstash.plugins.ConfigurationImpl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import tech.ydb.test.junit5.YdbHelperExtension;

public class YdbStoragePluginTest {

    @RegisterExtension
    private static final YdbHelperExtension ydb = new YdbHelperExtension();

    private static String connectionString() {
        StringBuilder connect = new StringBuilder()
                .append(ydb.useTls() ? "grpcs://" : "grpc://")
                .append(ydb.endpoint())
                .append(ydb.database());
        return connect.toString();
    }

    private static Configuration getConfiguration() {
        String connectionString = connectionString();
        String tableName = "table_sample";
        String column = "time, Timestamp, number, Int64, date, Datetime, smallNumber, Int16";
        Map<String, Object> configValues = new HashMap<>();
        configValues.put(YdbStoragePlugin.CONNECTION_STRING.name(), connectionString);
        configValues.put(YdbStoragePlugin.TABLE_NAME.name(), tableName);
        configValues.put(YdbStoragePlugin.COLUMNS.name(), column);
        configValues.put(YdbStoragePlugin.CREATE_TABLE.name(), true);
        return new ConfigurationImpl(configValues);
    }

    @Test
    public void testJavaOutputExample() {
        Configuration config = getConfiguration();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        YdbStoragePlugin output = new YdbStoragePlugin("id", config, null, baos);

        String sourceField = "message";
        int eventCount = 5;
        Collection<Event> events = new ArrayList<>();
        for (int k = 0; k < eventCount; k++) {
            Event e = new org.logstash.Event();
            e.setField(sourceField, "message " + k);
            Long time = Instant.now().getEpochSecond();
            e.setField("time", time);
            Long number = 100L + k;
            e.setField("number", number);
            e.setField("date", time);
            Short smallNumber = 1;
            e.setField("smallNumber", smallNumber);
            events.add(e);
        }

        output.output(events);

        String outputString = baos.toString();
        int index = 0;
        int lastIndex = 0;
        while (index < eventCount) {
            lastIndex = outputString.indexOf("message", lastIndex);
            Assertions.assertTrue(lastIndex > -1);
            lastIndex = outputString.indexOf("message " + index);
            Assertions.assertTrue(lastIndex > -1);
            index++;
        }
    }
}