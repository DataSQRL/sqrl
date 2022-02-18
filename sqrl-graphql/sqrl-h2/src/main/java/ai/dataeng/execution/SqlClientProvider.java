package ai.dataeng.execution;

import io.vertx.sqlclient.SqlClient;

public interface SqlClientProvider {
    SqlClient get();
}
