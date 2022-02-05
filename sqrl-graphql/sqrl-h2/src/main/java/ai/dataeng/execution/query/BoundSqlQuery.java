package ai.dataeng.execution.query;

import io.vertx.core.Future;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import lombok.Value;

@Value
public class BoundSqlQuery {
  SqlClient client;
  H2SingleQuery query;
  Optional<Tuple> arguments;
  public CompletionStage execute() {
    PreparedQuery<RowSet<Row>> preparedQuery = client.preparedQuery(query.getQuery());

    Future<RowSet<Row>> result;
    if (arguments.isPresent()) {
      System.out.println(arguments.get().deepToString());
      result = preparedQuery.execute(arguments.get());
    } else {
      result = preparedQuery.execute();
    }

    try {
      Object o = result
          .map(query.getResultMapper())
          .toCompletionStage().toCompletableFuture().get();
      System.out.println(o);
    } catch (Exception e){
      e.printStackTrace();
    }

    return result
        .map(query.getResultMapper())
        .toCompletionStage()
        .exceptionally(e->{
          e.printStackTrace();
          return e;
        });
  }
}
