package ai.datasqrl.parse.tree;

import java.util.Optional;

public abstract class Declaration extends Node {
  protected Declaration(Optional<NodeLocation> location) {
    super(location);
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDeclaration(this, context);
  }
}
