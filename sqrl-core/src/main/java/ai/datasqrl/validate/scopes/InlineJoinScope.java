package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.validate.Namespace;
import java.util.Map;
import lombok.Value;

@Value
public class InlineJoinScope implements ValidatorScope {
  Map<Node, ValidatorScope> scopes;
  @Override
  public Namespace getNamespace() {
    return null;
  }
}
