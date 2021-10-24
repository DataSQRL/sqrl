package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.ViewQueryRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ColumnNameGen;
import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.ViewScope;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.logical3.LogicalPlan2.DataField;
import ai.dataeng.sqml.logical3.LogicalPlan2.DelegateLogicalField;
import ai.dataeng.sqml.logical3.LogicalPlan2.DistinctRelationField;
import ai.dataeng.sqml.logical3.LogicalPlan2.LogicalField;
import ai.dataeng.sqml.logical3.LogicalPlan2.RelationshipField;
import ai.dataeng.sqml.logical3.LogicalPlan2.SelectRelationField;
import ai.dataeng.sqml.logical3.LogicalPlan2.SubscriptionField;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.JoinAssignment;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final AtomicBoolean importResolved = new AtomicBoolean(false);
    private final PhysicalModel plan = new PhysicalModel();
    private final ColumnNameGen columnNameGen = new ColumnNameGen();

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

      List<Node> statements = node.getStatements();
      for (int i = 0; i < statements.size(); i++) {
        statements.get(i).accept(this, scope);
        tryCompleteImportHeader(statements, i, scope);
      }

      analysis.setLogicalPlan2(scope.getPlanBuilder().build());

      return null;
    }

    @Override
    protected Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getQualifiedName()));
      }

      ImportManager importManager = new ImportManager(metadata.getEnv()
          .getDdRegistry());

      //TODO: One import set per line
      if (node.getQualifiedName().getParts().size() == 1) {
        if (node.getQualifiedName().getParts().get(0).equalsIgnoreCase("*")) {
          throw new RuntimeException("Cannot import * at base level");
        }
        importManager.importDataset(node.getQualifiedName().getParts().get(0), node.getAlias().map(Identifier::getValue));
      } else if (node.getQualifiedName().getParts().size() > 1) {
        if (node.getQualifiedName().getParts().get(node.getQualifiedName().getParts().size() - 1)
            .equalsIgnoreCase("*")) {
          importManager.importAllTable(node.getQualifiedName().getParts().get(0));
        } else {
          importManager.importTable(node.getQualifiedName().getParts().get(0),
              node.getQualifiedName().getParts().get(1), node.getAlias().map(Identifier::getValue));
        }
      }

      ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
      ImportSchema schema = importManager.createImportSchema(errors);

      //schema.getSchema().getFieldByName(mapping.getKey()))
      for (Map.Entry<Name, Mapping> mapping : schema.getMappings().entrySet()) {
        scope.addRootField(new DelegateLogicalField(schema.getSchema().getFieldByName(mapping.getKey())));
      }

      createPhysicalView(node, scope, Optional.of(schema), Optional.empty(), Optional.empty());

      return scope;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      QualifiedName name = queryAssignment.getName();
      Query query = queryAssignment.getQuery();
      Scope newScope = createAndAssignScope(query, null, name, scope);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata);
      Scope result = query.accept(statementAnalyzer, newScope);

      scope.addField(new SelectRelationField(toName(name), result.getRelation(), Optional.empty()));

      createPhysicalView(queryAssignment, result, Optional.empty(), Optional.of(statementAnalyzer.getAnalysis()),
          Optional.empty());

      return createAndAssignScope(queryAssignment, null,
          name, scope);
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        final Scope scope) {
      QualifiedName name = expressionAssignment.getName();

      Expression expression = expressionAssignment.getExpression();
      Scope assignedScope = createAndAssignScope(expression,
          null, name, scope);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expression, assignedScope);

      Type type = exprAnalysis.getType(expression);

      scope.addField(new DataField(toName(name), type, List.of()));

      createPhysicalView(expressionAssignment, scope, Optional.empty(),
          Optional.empty(), Optional.of(exprAnalysis));

      return createAndAssignScope(expression,
          null,
          name, scope);
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription subscription, Scope scope) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata);
      Scope queryScope = subscription.getQuery().accept(statementAnalyzer, scope);

      scope.addRootField(new SubscriptionField(
          toName(subscription.getName()), queryScope.getCurrentRelation()));

      return createAndAssignScope(subscription,
          null,
          subscription.getName(), scope);
    }

    @Override
    public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {
      RelationType<LogicalField> table = scope.resolveRelation(QualifiedName.of(node.getTable()))
          .orElseThrow(()->new RuntimeException(String.format("Could not find table %s", node.getTable().getValue())));

      scope.addField(new DistinctRelationField(toName(node.getName()), table));
      return scope;
    }

    @Override
    public Scope visitJoinAssignment(JoinAssignment node, Scope scope) {
      visitInlineJoin(node.getInlineJoin(), scope);

      scope.addField(new RelationshipField(toName(node.getName()), null, null));

      return createAndAssignScope(node, null, node.getName(), scope);
    }

    @Override
    public Scope visitInlineJoin(InlineJoin node, Scope context) {
      return super.visitInlineJoin(node, context);
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      this.analysis.addTypes(analysis.typeMap);

      return analysis;
    }

    private Scope createAndAssignScope(Node node, RelationType relationType, QualifiedName contextName,
        Scope parentScope) {
      Scope scope = Scope.builder()
          .withParent(parentScope)
          .withRelationType(relationType)
          .withContextName(contextName)
          .build();

      analysis.setScope(node, scope);
      return scope;
    }

    private void tryCompleteImportHeader(List<Node> statements,
        int i, Scope scope) {
      //Test for end of imports
      Optional<Node> nextStatement = getNextStatement(statements, i + 1);
      if (nextStatement.map(s->!(s instanceof ImportDefinition))
          .orElse(false)) {
        resolveImports(scope);
      }
    }

    private void resolveImports(Scope scope) {
      importResolved.set(true);
    }

    private Optional<Node> getNextStatement(List<Node> statements, int i) {
      if (i < statements.size()) {
        return Optional.of(statements.get(i));
      }
      return Optional.empty();
    }

    public Name toName(QualifiedName name) {
      return Name.of(name.toOriginalString(), NameCanonicalizer.SYSTEM);
    }

    private void createPhysicalView(Node node, Scope scope,
        Optional<ImportSchema> schema, Optional<StatementAnalysis> analysis,
        Optional<ExpressionAnalysis> expressionAnalysis) {
      ViewQueryRewriter viewRewriter = new ViewQueryRewriter(plan, columnNameGen);
      ViewScope scope2 = node.accept(viewRewriter, new ViewRewriterContext(scope, analysis,
          schema, expressionAnalysis));

      plan.addTable(scope2.getTable());
    }
  }
}
