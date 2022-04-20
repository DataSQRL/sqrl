package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.DistinctAssignment;
import ai.datasqrl.schema.Column;
import ai.datasqrl.transform.SqrlQueries;
import ai.datasqrl.validate.scopes.DistinctScope;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;

public class TransformToDistinct {

  @SneakyThrows
  public SqlNode transform(DistinctAssignment node, DistinctScope distinctScope) {
    String sql = SqrlQueries.generateDistinct(node,
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
}
