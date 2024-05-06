package tech.ydb.logstash;

import tech.ydb.auth.AuthProvider;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public interface YdbClient extends AutoCloseable {
    interface Factory {
        YdbClient create(String connectionString, AuthProvider auth);
    }

    String getDatabase();
    
    TableDescription desribeTable(String tablePath);

    void bulkUpsert(String tablePath, ListValue messages);

    @Override
    void close();
}
