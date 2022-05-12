package ai.datasqrl.plan.local.transpiler.nodes.schemaRef;

import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.schema.Table;
import lombok.Getter;

@Getter
public class SelfRef extends TableRef {

  private final RelationNorm self;

  public SelfRef(Table table, RelationNorm self) {
    super(table);
    this.self = self;
  }
}
