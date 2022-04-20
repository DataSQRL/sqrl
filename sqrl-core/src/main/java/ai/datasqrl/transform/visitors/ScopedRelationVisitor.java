package ai.datasqrl.transform.visitors;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.RelationRewriter;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.validate.scopes.FunctionCallScope;
import ai.datasqrl.validate.scopes.IdentifierScope;
import ai.datasqrl.validate.scopes.StatementScope;
import ai.datasqrl.validate.scopes.TableScope;
import ai.datasqrl.validate.scopes.ValidatorScope;
import com.google.common.base.Preconditions;
import org.checkerframework.checker.units.qual.C;

public class ScopedRelationVisitor extends AstVisitor<Relation, Object> {
  private final StatementScope scope;

  public ScopedRelationVisitor(StatementScope scope) {
    this.scope = scope;
  }

  @Override
  public Relation visitTableNode(TableNode node, Object context) {
    ValidatorScope validatorScope = scope.getScopes().get(node);
    Preconditions.checkState(validatorScope instanceof TableScope);
    return rewriteTableNode(node, (TableScope) validatorScope);
  }

  public <T extends Relation> T rewriteTableNode(TableNode functionCall, TableScope tableScope) {
    return null;
  }
}
