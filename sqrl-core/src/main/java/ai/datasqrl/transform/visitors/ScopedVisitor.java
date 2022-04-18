package ai.datasqrl.transform.visitors;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.validate.scopes.FunctionCallScope;
import ai.datasqrl.validate.scopes.IdentifierScope;
import ai.datasqrl.validate.scopes.StatementScope;
import ai.datasqrl.validate.scopes.TableScope;

public class ScopedVisitor<T, C> extends AstVisitor<T, C> {

  private final StatementScope scope;

  public ScopedVisitor(StatementScope scope) {
    this.scope = scope;
  }

  public <T extends Expression> T rewriteIdentifier(Identifier identifier, IdentifierScope identifierScope) {
    return null;
  }

  public <T extends Expression> T rewriteFunction(FunctionCall functionCall, FunctionCallScope functionCallScope) {
    return null;
  }

  public <T extends Expression> T rewriteTableNode(TableNode functionCall, TableScope tableScope) {
    return null;
  }


  public Node getScope(Node node) {
    return null;
  }

}
