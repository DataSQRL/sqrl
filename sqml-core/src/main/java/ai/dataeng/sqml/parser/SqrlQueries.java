package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.stream.Collectors;

public class SqrlQueries {
  static AliasGenerator gen = new AliasGenerator();

  public static String generateDistinct(DistinctAssignment statement, Table refTable, List<String> partition, List<String> columns) {
    //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/
    String template = "SELECT %s FROM (SELECT * FROM ("
        + " SELECT *, ROW_NUMBER() OVER (PARTITION BY %s %s) __row_number "
        + " FROM %s) %s "
        + "WHERE __row_number = 1) %s";

    String columnsStr = String.join(", ", columns);
    String partitionStr = String.join(", ", partition);

    List<String> order = statement.getOrder().stream()
        .map(o->refTable.getField(Name.system(o.getSortKey().toString())).getId() + o.getOrdering().map(dir->" " + ((dir == Ordering.DESCENDING) ? "DESC" : "ASC")).orElse(""))
        .collect(Collectors.toList());
    String orderStr = order.isEmpty() ? " " : " ORDER BY " + String.join(", ", order);

    String query = String.format(template,
        columnsStr,
        partitionStr, orderStr,
        refTable.getId().toString(),
        gen.nextTableAliasName().toString(),
        gen.nextTableAliasName().toString()
    );
    return query;
  }
}
