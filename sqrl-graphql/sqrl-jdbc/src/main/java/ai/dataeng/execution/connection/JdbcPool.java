package ai.dataeng.execution.connection;

import io.vertx.sqlclient.SqlClient;
import lombok.Value;

@Value
public class JdbcPool implements Pool {

  //These are vertx pools so we this needs to be passed in
  SqlClient sqlClient;
}
