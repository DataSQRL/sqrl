package ai.datasqrl.graphql.execution.query;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;

@Value
public class H2SingleQuery {
  String query;
  Optional<Tuple> args;
  Function<RowSet<Row>, Object> resultMapper;
}