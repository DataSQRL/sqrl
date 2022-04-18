package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.validate.Namespace;
import java.util.List;
import lombok.Value;

@Value
public class SelectScope implements ValidatorScope {
  Select select;
  private List<Expression> expandedSelectList;
  private Expression orders;

  @Override
  public Namespace getNamespace() {
    return null;
  }
}
