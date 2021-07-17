package ai.dataeng.sqml.tree;

public class NodeFormatter extends AstVisitor<String, Void> {
  private static final NodeFormatter nodeFormatter = new NodeFormatter();
  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  protected String visitNode(Node node, Void context) {
    return "{}";
  }

  @Override
  protected String visitStringLiteral(StringLiteral node, Void context) {
    return node.getValue();
  }

  @Override
  protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitIdentifier(Identifier node, Void context) {
    return node.getValue();
  }
}
