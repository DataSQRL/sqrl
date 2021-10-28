package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.ExpressionAnalysis;
import ai.dataeng.sqml.tree.Expression;

public class ExpressionSqlizer {
  private final Analysis analysis;

  public ExpressionSqlizer(Analysis analysis) {
    this.analysis = analysis;
  }

  /**
   * Converts an expression to its sql equivalent.
   */
  public ExpressionSqlizerAnalysis rewrite(Expression expression, ExpressionAnalysis expressionAnalysis) {
    // sum(entries.total)
    // => select sum(entries.total) from @;
    // => select sum(e1.total) from @ outer join @.entries as e1 on pk;

    return null;
  }
}
