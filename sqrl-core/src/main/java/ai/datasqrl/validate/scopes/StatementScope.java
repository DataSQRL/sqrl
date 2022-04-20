package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Table;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import org.apache.commons.collections.map.ListOrderedMap;

@Value
public class StatementScope {
  Optional<Table> contextTable;
  NamePath namePath;
  Map<Node, ValidatorScope> scopes;
}
