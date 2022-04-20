package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.parse.tree.SortItem.Ordering;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import ai.datasqrl.util.AliasGenerator;
import ai.datasqrl.validate.scopes.DistinctScope;
import java.util.List;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public class DistinctToSqlNode {

  @SneakyThrows
  public SqlNode transform(DistinctAssignment node, DistinctScope distinctScope) {
    String sql = generateDistinct(node,
        distinctScope.getTable(),
        distinctScope.getPartitionKeys().stream()
          .map(name->name.getId().toString())
          .collect(Collectors.toList()),
        distinctScope.getTable().getFields().getElements().stream()
          .filter(field->field instanceof Column)
          .map(e->e.getId().toString())
          .collect(Collectors.toList())
    );

    SqlParser parser = SqlParser.create(sql);

    return parser.parseQuery();
  }

  public static String generateDistinct(DistinctAssignment statement, Table refTable, List<String> partition, List<String> fields) {
    //https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/deduplication/
    String template = "SELECT %s FROM (SELECT * FROM ("
        + " SELECT *, ROW_NUMBER() OVER (PARTITION BY %s %s) __row_number "
        + " FROM %s) %s "
        + "WHERE __row_number = 1) %s";

    String columnsStr = String.join(", ", fields);
    String partitionStr = String.join(", ", partition);

    List<String> order = statement.getOrder().stream()
        .map(o->refTable.getField(Name.system(o.getSortKey().toString())).getId()
            + o.getOrdering().map(dir->" " + ((dir == Ordering.DESCENDING) ? "DESC" : "ASC")).orElse(""))
        .collect(Collectors.toList());
    String orderStr = order.isEmpty() ? " " : " ORDER BY " + String.join(", ", order);

    AliasGenerator gen = new AliasGenerator();
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
