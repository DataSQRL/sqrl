package ai.datasqrl.graphql.execution.query;

import ai.datasqrl.graphql.execution.page.PageProvider;
import ai.datasqrl.graphql.execution.table.H2Table;
import com.google.common.collect.ArrayListMultimap;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import lombok.Value;

@Value
public class BatchQueryBuilder {
  H2Table table;
  PageProvider pageProvider;

  public H2SingleQuery build(List<Object> environment) {
    Object arr[][] = {new Object[]{1}, new Object[]{2}};

    return new H2SingleQuery(
        "select customerid, entries_pos, discount from entries where (customerid) = ANY(?)",
        Optional.of(Tuple.of(arr)),
        new Function<RowSet<Row>,Object>() {
          @Override
          public List<Object> apply(RowSet<Row> rowSet) {
            ArrayListMultimap<Object, Map> multiMap = ArrayListMultimap.create();

            for (Row r : rowSet) {
              multiMap.put(
                  r.getInteger(0),
                  Map.of("discount", r.getFloat("discount")));
            }

            //Reorder the results to the original order
            List result = new ArrayList<>();
            for (Object[] key : arr) {
              result.add(pageProvider.wrap((List)multiMap.get(key[0]), "test", false));
            }

            return result;
          }
        }
    );
  }
}
