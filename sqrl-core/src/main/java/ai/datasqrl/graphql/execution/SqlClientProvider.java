package ai.datasqrl.graphql.execution;

import io.vertx.sqlclient.SqlClient;

public interface SqlClientProvider {

  SqlClient get();
}
