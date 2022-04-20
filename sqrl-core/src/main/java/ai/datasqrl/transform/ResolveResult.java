package ai.datasqrl.transform;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Table;
import java.util.Optional;
import lombok.Value;

@Value
public class ResolveResult {

  Field firstField;
  Optional<NamePath> remaining;
  Name alias;
  Table table;
}
