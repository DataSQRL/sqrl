package ai.datasqrl.plan.local.transpiler.nodes.schemaRef;

import ai.datasqrl.schema.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableRef implements TableOrRelationship {
  Table table;
}
