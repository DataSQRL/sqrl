package ai.datasqrl.plan.local.transpiler.nodes.schemaRef;

import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import lombok.Value;

@Value
public class RelationshipRef implements TableOrRelationship {

  Relationship relationship;

  @Override
  public Table getTable() {
    return relationship.getToTable();
  }
}
