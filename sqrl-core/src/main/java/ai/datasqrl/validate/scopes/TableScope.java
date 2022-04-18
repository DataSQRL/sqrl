package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.validate.Namespace;
import ai.datasqrl.validate.paths.TablePath;
import ai.datasqrl.validate.scopes.ValidatorScope;
import lombok.Value;

@Value
public class TableScope implements ValidatorScope {

  TableNode tableNode;
  TablePath tablePath;
  Name alias;

  @Override
  public Namespace getNamespace() {
    return null;
  }
}
