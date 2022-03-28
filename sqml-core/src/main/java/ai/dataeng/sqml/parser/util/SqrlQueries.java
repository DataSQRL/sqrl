package ai.dataeng.sqml.parser.util;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import java.util.List;
import java.util.stream.Collectors;

public class SqrlQueries {
  static AliasGenerator gen = new AliasGenerator();

  public static String generateDistinct(DistinctAssignment statement, List<String> partition) {
    //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/
    String template = "SELECT * FROM ("
        + " SELECT *, ROW_NUMBER() OVER (PARTITION BY %s %s) __row_number "
        + " FROM %s) %s "
        + "WHERE __row_number = 1";

    String partitionStr = String.join(", ", partition);

    List<String> order = statement.getOrder().stream()
        .map(o->"`" + o.getSortKey().toString() +"`" + o.getOrdering().map(dir->" " + ((dir == Ordering.DESCENDING) ? "DESC" : "ASC")).orElse(""))
        .collect(Collectors.toList());
    String orderStr = order.isEmpty() ? " " : " ORDER BY " + String.join(", ", order);

    String query = String.format(template,
        partitionStr, orderStr,
        statement.getTable().getCanonical(),
        gen.nextTableAlias()
    );
    return query;
  }
}
