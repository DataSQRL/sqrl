package ai.datasqrl.validate;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Optional;

/**
 * The namespace allows the script to resolve imported scripts, functions, and datasets that are not
 * immediately brought into scope.
 */
public class Namespace {

  /**
   * Adds a schema object to the namespace, so it can be referenced later
   */
  public void exposeSchemaObject(Optional<Name> alias, Object object) {

  }

  public Object resolveSchemaObject(NamePath namePath) {
    return null;
  }
}
