package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.transform.visitors.ScopedVisitor;
import ai.datasqrl.validate.scopes.StatementScope;
import org.apache.calcite.sql.validate.SelectScope;

/**
 * Expands selects to include
 * - Order by expressions that cannot be mapped to ordinals
 * - SELECT *
 */
public class ExpandSelect extends ScopedVisitor {

  public ExpandSelect(StatementScope scope) {
    super(scope);
  }

  public Select rewriteSelect(Select select, SelectScope selectScope) {

    return null;
  }
}
