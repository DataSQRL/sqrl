package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.Table;
import ai.datasqrl.validate.Namespace;
import java.util.Map;
import lombok.Value;

@Value
public class QuerySpecScope implements ValidatorScope {

  Map<TableNode, TableScope> tableScopes;
  Map<Name, Table> joinScopes;

  @Override
  public Namespace getNamespace() {
    return null;
  }
}
