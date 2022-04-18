package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.schema.Table;
import java.util.Map;
import java.util.Optional;
import lombok.Value;

@Value
public class StatementScope {
  Optional<Table> contextTable;
  Map<Node, ValidatorScope> scopes;
}
