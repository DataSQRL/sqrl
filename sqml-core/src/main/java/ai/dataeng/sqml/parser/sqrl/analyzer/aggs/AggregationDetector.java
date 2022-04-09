package ai.dataeng.sqml.parser.sqrl.analyzer.aggs;

import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Node;

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
    if (functionLookup.lookup(node.getName()).isAggregate()) {
      this.agg = true;
    }
    return super.visitFunctionCall(node, context);
  }
}
