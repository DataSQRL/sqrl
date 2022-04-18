package ai.datasqrl.validate.aggs;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Node;

public class AggregationDetector extends DefaultTraversalVisitor<Void, Void> {
  private final FunctionLookup functionLookup;
  public AggregationDetector(FunctionLookup functionLookup) {
    this.functionLookup = functionLookup;
  }

  boolean agg = false;

  public boolean isAggregating(Node rewritten) {
    this.agg = false;
    rewritten.accept(this, null);
    return this.agg;
  }

  @Override
  public Void visitFunctionCall(FunctionCall node, Void context) {
    if (node.getOver().isPresent()) {
      return super.visitFunctionCall(node, context);
    }

    if (functionLookup.lookup(node.getNamePath()).isAggregate()) {
      this.agg = true;
    }
    return super.visitFunctionCall(node, context);
  }
}
