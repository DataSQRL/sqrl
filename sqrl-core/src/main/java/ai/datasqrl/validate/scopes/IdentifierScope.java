package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Field;
import ai.datasqrl.validate.Namespace;
import java.util.List;
import lombok.Value;

@Value
public class IdentifierScope extends ExpressionScope {
  Name alias;
  NamePath qualifiedName;
  List<Field> fieldPath;
  ResolveResult resolveResult;


  @Override
  public Namespace getNamespace() {
    return null;
  }
}
