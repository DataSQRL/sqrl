package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Node;

public class TranslationMapper extends AstVisitor<Node, Node> {

  @Override
  protected Node visitNode(Node node, Node context) {
    return node;
  }
}
