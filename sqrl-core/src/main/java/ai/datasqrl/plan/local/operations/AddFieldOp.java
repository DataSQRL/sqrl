package ai.datasqrl.plan.local.operations;

import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

@Value
public class AddFieldOp implements SchemaUpdateOp {
  private final Table table;
  private final Field field;
  //replaces the rel node of table
  private final Optional<RelNode> relNode;

  @Override
  public <T> T accept(SchemaOpVisitor visitor) {
    return visitor.visit(this);
  }
}
