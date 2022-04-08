package ai.dataeng.sqml.parser.sqrl.analyzer;

import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.FunctionCall;

public class AggregationVisitor extends AstVisitor {
  FunctionLookup functionLookup = new FunctionLookup();
  boolean agg = false;
  public boolean hasAgg() {
    return agg;
  }

  @Override
  public Object visitFunctionCall(FunctionCall node, Object context) {
    if (functionLookup.lookup(node.getName()).isAggregate()) {
      this.agg = true;
    }
    return super.visitFunctionCall(node, context);
  }
}
