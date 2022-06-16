package ai.datasqrl.plan.calcite;

import lombok.AllArgsConstructor;
import lombok.experimental.Delegate;
import org.apache.calcite.schema.SchemaPlus;

@AllArgsConstructor
public class SqrlSchemaCatalog {
  @Delegate
  SchemaPlus rootSchema;
}
