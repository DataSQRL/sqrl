package com.datasqrl.calcite.visitor;

import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.Sort;

/**
 * A different visitor pattern for relnodes
 */
public class RelRewriter<C> extends AbstractRelNodeVisitor<RelNode, C> {

  @Override
  public RelNode visitRelNode(RelNode node, C context) {
    return node.copy(node.getTraitSet(), visitChildren(node, context));
  }

  private List<RelNode> visitChildren(RelNode node, C context) {
    return node.getInputs().stream()
        .map(n->accept(this, n, context))
        .collect(Collectors.toList());
  }
}