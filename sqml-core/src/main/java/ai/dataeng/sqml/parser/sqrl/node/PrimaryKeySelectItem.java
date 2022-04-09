package ai.dataeng.sqml.parser.sqrl.node;

import ai.dataeng.sqml.parser.Column;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.SingleColumn;
import java.util.Optional;

public class PrimaryKeySelectItem
    extends SingleColumn {

  private final Column column;

  public PrimaryKeySelectItem(Identifier expression, Identifier alias, Column column) {
    super(Optional.empty(), expression, Optional.of(alias));
    this.column = column;
  }

  public Column getColumn() {
    return column;
  }

  public Identifier getIdentifier() {
    return (Identifier) getExpression();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
