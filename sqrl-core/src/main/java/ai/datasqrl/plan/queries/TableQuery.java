package ai.datasqrl.plan.queries;

import ai.datasqrl.schema.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

/**
 * A query in the logical plan.
 */
@AllArgsConstructor
@Getter
public class TableQuery {

  Table table;
  RelNode relNode;
}