package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.RelationType.NamedRelationType;
import ai.dataeng.sqml.type.RelationType.RootRelationType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.RelationType;
import org.apache.flink.util.Preconditions;

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
      Scope scope = new Scope(new RootRelationType(new RelationType()));
      node.getStatements()
          .forEach(n -> n.accept(this, scope));
      this.analysis.setModel(scope.getRoot());
      return null;
    }

    @Override
    protected Scope visitImport(Import node, Scope context) {
      ImportAnalyzer importResolver = new ImportAnalyzer(this.metadata);
      Scope scope = importResolver.analyzeImport(node, context);
      return scope;
    }

    @Override
    protected Scope visitAssign(Assign node, Scope scope) {
      Scope result = node.getRhs().accept(this, createScope(scope, node.getName()));
      return result;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      //Query assignments statements operate on the root scope
      //Scope newScope = createAndAssignScope(queryAssignment, scope, scope.getRoot());
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata, this.analysis);
      Scope result = statementAnalyzer.analyze(queryAssignment.getQuery(), scope);
      RelationType rel = new NamedRelationType(result.getRelation(), scope.getName().getSuffix());
      scope.addRelation(scope.getName(), rel);

      return result;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        final Scope scope) {
      RelationType rel = scope.getName().getPrefix()
          .map(p -> scope.resolveRelation(p)
              .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", scope.getName().getPrefix()))))
          .orElse(scope.getRoot());

      Scope assignedScope = createAndAssignScope(expressionAssignment.getExpression(), scope, rel);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expressionAssignment.getExpression(), assignedScope, true);

      Type type = exprAnalysis.getType(expressionAssignment.getExpression());
      Preconditions.checkNotNull(type, "Could not find type for %s %s",
            expressionAssignment.getExpression().getClass().getName(),
            expressionAssignment.getExpression());

      rel.addField(Field.newDataField(assignedScope.getName().getSuffix(), type));
      return createAndAssignScope(expressionAssignment.getExpression(), assignedScope, rel);
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription node, Scope context) {
      return null;
    }

    @Override
    public Scope visitDistinctAssignment(DistinctAssignment node, Scope context) {
      //todo: add primary key
      return context;
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope, boolean allowFieldCreation) {
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
          .withRelationType(scope.getRoot())
          .build();
    }

    private Scope createAndAssignScope(Node node, Scope parentScope, RelationType relationType)
    {
      Scope scope = Scope.builder()
          .withParent(parentScope)
          .withRelationType(relationType)
          .withName(parentScope.getName())
          .build();

      analysis.setScope(node, scope);
      return scope;
    }
  }
}
