package ai.datasqrl.plan.queries;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.schema.SQRLTable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

/**
 * A query in the logical plan.
 */
@AllArgsConstructor
@Getter
public class TableQuery {

  TableWithPK table;
  RelNode relNode;
}