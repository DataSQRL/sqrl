package ai.dataeng.sqml.graphql;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public class SingleQuery {

  public String getQuery() {
    return "select customerid_col from orders;";
  }

  public Optional<Tuple> getArguments() {
    return Optional.empty();
  }

  public Function<RowSet<Row>, List<Object>> getResultMapper() {
    return new Function<RowSet<Row>, List<Object>>() {
      //Good codegen candidate
      @Override
      public List<Object> apply(RowSet<Row> rows) {
        List results = new ArrayList();
        for (Row row : rows) {
          results.add(Map.of("customerid", row.getInteger("customerid_col")));
        }

        return results;
      }
    };
  }
}
