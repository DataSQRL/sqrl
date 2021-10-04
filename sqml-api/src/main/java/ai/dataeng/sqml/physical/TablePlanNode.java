package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.logical.RelationDefinition;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
public class TablePlanNode extends PhysicalPlanNode {
  protected String tableName;
  protected List<Column> columns;

  public TablePlanNode(RelationDefinition relationDefinition, String tableName, List<Column> columns) {
    super(relationDefinition);
    this.tableName = tableName;
    this.columns = columns;
  }

  public <R, C> R accept(PhysicalPlanVisitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
//
//  public Optional<Column> getColumn(String name) {
//    for (Column column : columns) {
//      if (column.getName().equalsIgnoreCase(name)) {
//        return Optional.of(column);
//      }
//    }
//    return Optional.empty();
//  }
}
