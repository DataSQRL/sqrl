package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.expression.ExpressionAnalysis;
import ai.dataeng.sqml.expression.ExpressionAnalyzer;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import com.google.common.cache.AbstractCache;
import java.util.List;
import java.util.Map;

public class Analyzer {
  private final Metadata metadata;
  private final Analysis analysis;
  private final Script script;
  private final List<QuerySpecification> var = null;

  private Analyzer(Script script, Metadata metadata, Analysis analysis) {
    this.script = script;
    this.metadata = metadata;
    this.analysis = analysis;
  }

  public static Analysis analyze(Script script, Metadata metadata) {
    Analysis analysis = new Analysis(script);
    Analyzer analyzer = new Analyzer(script, metadata, analysis);

    analyzer.analyze();

    return analysis;
  }

  public void analyze() {
    Visitor visitor = new Visitor();
    script.accept(visitor, null);
  }

  class Visitor extends DefaultTraversalVisitor<Scope, Scope> {

    @Override
    protected Scope visitAssign(Assign node, Scope context) {
      node.getRhs().accept(this, new Scope(node.getName()));

      return null;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis exprAnalysis = analyzer.analyze(expressionAssignment.getExpression(), scope);
      SqmlType type = exprAnalysis.getType(expressionAssignment.getExpression());
      return createType(scope, expressionAssignment.getExpression(), type);
    }

    private Scope createType(Scope scope, Expression expression, SqmlType type) {
      analysis.setType(expression, type);
      return scope;
    }
  }
}
