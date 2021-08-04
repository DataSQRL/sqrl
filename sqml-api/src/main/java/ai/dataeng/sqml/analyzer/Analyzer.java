package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.Statement;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;

public class Analyzer {
  protected final Metadata metadata;
  protected final Analysis analysis;
  protected final Script script;

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
    Visitor visitor = new Visitor(analysis, metadata);
    script.accept(visitor, null);
  }

  public static class Visitor extends AstVisitor<Scope, Scope> {

    private final Analysis analysis;
    private final Metadata metadata;

    public Visitor(Analysis analysis, Metadata metadata) {
      this.analysis = analysis;
      this.metadata = metadata;
    }

    @Override
    protected Scope visitNode(Node node, Scope context) {
      throw new RuntimeException(String.format("Could not process node %s : %s", node.getClass().getName(), node));
    }

    @Override
    protected Scope visitScript(Script node, Scope context) {
      Scope scope = new Scope();
      node.getStatements()
          .forEach(n -> n.accept(this, scope));
      return null;
    }

    @Override
    protected Scope visitAssign(Assign node, Scope scope) {
      Scope result = node.getRhs().accept(this, createScope(scope, node.getName()));

      if (result.getRelationType() == null) {
        throw new RuntimeException(String.format(
            "Temporary exception, unknown type: %s, %s", node, scope));
      }

      return result;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      Scope result = analyzeStatement(queryAssignment.getQuery(), scope);
      RelationSqmlType rel = scope.createRelation(scope.getName().getPrefix().get());
      rel.addField(Field.newUnqualified(scope.getName().getSuffix(), result.getRelationType()));
      return result;
    }

    private Scope analyzeStatement(Statement statement, Scope scope) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata, this.analysis);
      return statementAnalyzer.analyze(statement, scope);
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Scope scope) {
      RelationSqmlType rel = scope.createRelation(scope.getName().getPrefix().get());
      scope = createAndAssignScope(expressionAssignment.getExpression(), scope, rel);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expressionAssignment.getExpression(), scope);

      SqmlType type = exprAnalysis.getType(expressionAssignment.getExpression());
      if (type == null) {
        throw new RuntimeException(String.format("Could not find type for %s %s",
            expressionAssignment.getExpression().getClass().getName(),
            expressionAssignment.getExpression()
        ));
      }

      rel.addField(Field.newUnqualified(scope.getName().getSuffix(), type));
      return createAndAssignScope(expressionAssignment.getExpression(), scope, rel);
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      this.analysis.addTypes(analysis.typeMap);
      this.analysis.addRelations(analysis.relations);

      return analysis;
    }

    private Scope createScope(Scope scope, QualifiedName name) {
      return Scope.builder()
          .withName(name)
          .withParent(scope)
          .build();
    }

    private Scope createAndAssignScope(Node node, Scope parentScope, RelationSqmlType relationType)
    {
      Scope scope = Scope.builder()
          .withParent(parentScope)
          .withRelationType(node, relationType)
          .withName(parentScope.getName())
          .build();

      analysis.setScope(node, scope);
      return scope;
    }
  }
}
