package ai.dataeng.execution.orderby;

import ai.dataeng.execution.table.column.Columns;
import ai.dataeng.execution.table.column.H2Column;
import com.google.common.collect.Maps;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

//Non-batched ordering only (?)
public class H2OrderByProvider implements OrderByProvider {

  public Optional<List<String>> getOrderBy(DataFetchingEnvironment environment, Columns columns) {
    List<Map<String, Object>> orders = (List<Map<String, Object>>)environment.getArguments().get("order_by");

    if (orders == null) {
      return Optional.empty();
    }

    //Todo inverse column handling for ordering
    List<String> orderBySql = new ArrayList<>();
    Map<String, H2Column> columnMap = Maps.uniqueIndex(columns.getColumns(), c->c.getName());
    for (Map<String, Object> map : orders) {
      //Todo: validate if there are more than one object? Is this ordered?
      for (Map.Entry<String, Object> objectEntry : map.entrySet()) {
        orderBySql.add(String.format("%s %s", columnMap.get(objectEntry.getKey()).getPhysicalName(), objectEntry.getValue()));
      }
    }

    return Optional.of(orderBySql);
  }
}
