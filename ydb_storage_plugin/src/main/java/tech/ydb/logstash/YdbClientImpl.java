package tech.ydb.logstash;

import java.util.concurrent.atomic.AtomicBoolean;

import tech.ydb.auth.AuthProvider;
import tech.ydb.core.grpc.GrpcTransport;
import tech.ydb.table.SessionRetryContext;
import tech.ydb.table.TableClient;
import tech.ydb.table.description.TableDescription;
import tech.ydb.table.settings.BulkUpsertSettings;
import tech.ydb.table.values.ListValue;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class YdbClientImpl implements YdbClient {
    private final GrpcTransport transport;
    private final TableClient tableClient;
    private final SessionRetryContext retryCtx;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public YdbClientImpl(String connectionString, AuthProvider auth) {
        this.transport = GrpcTransport.forConnectionString(connectionString)
                .withAuthProvider(auth)
                .build();
        this.tableClient = TableClient.newClient(transport).build();
        this.retryCtx = SessionRetryContext.create(tableClient).build();
    }

    @Override
    public TableDescription desribeTable(String tablePath) {
        return retryCtx.supplyResult(session -> session.describeTable(tablePath)).join().getValue();
    }

    @Override
    public void bulkUpsert(String tablePath, ListValue messages) {
        retryCtx.supplyStatus(
                session -> session.executeBulkUpsert(tablePath, messages, new BulkUpsertSettings())
        ).join().expectSuccess("bulk upsert problem");
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            tableClient.close();
            transport.close();
        }
    }

    @Override
    public String getDatabase() {
        return transport.getDatabase();
    }
}
