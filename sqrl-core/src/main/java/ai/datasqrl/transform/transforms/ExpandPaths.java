package ai.datasqrl.transform.transforms;

import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.transform.visitors.ScopedVisitor;
import ai.datasqrl.validate.scopes.FunctionCallScope;
import ai.datasqrl.validate.scopes.IdentifierScope;
import ai.datasqrl.validate.scopes.StatementScope;

/**
 * Expands to-one paths and to-many paths
 */
public class ExpandPaths extends ScopedVisitor {

  public ExpandPaths(StatementScope scope) {
    super(scope);
  }

  @Override
  public Identifier rewriteIdentifier(Identifier identifier, IdentifierScope identifierScope) {
    return null;
  }

  @Override
  public FunctionCall rewriteFunction(FunctionCall functionCall, FunctionCallScope functionCallScope) {
    return null;
  }
}
