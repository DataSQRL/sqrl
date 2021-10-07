package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.logical.DistinctRelationDefinition;
import ai.dataeng.sqml.logical.ImportRelationDefinition;
import ai.dataeng.sqml.logical.RelationIdentifier;
import ai.dataeng.sqml.logical.ExtendedChildQueryRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedFieldRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildRelationDefinition;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.QueryField;
import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.CreateSubscription;
import ai.dataeng.sqml.tree.DistinctAssignment;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.InlineJoin;
import ai.dataeng.sqml.tree.InlineJoinBody;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private final AtomicBoolean importResolved = new AtomicBoolean(false);
    private ImportManager importManager;

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
      this.importManager = new ImportManager(metadata.getEnv()
          .getDdRegistry());
      Scope scope = new Scope(analysis.getLogicalPlan());

      List<Node> statements = node.getStatements();
      for (int i = 0; i < statements.size(); i++) {
        statements.get(i).accept(this, scope);

        //Test for end of imports
        Optional<Node> nextStatement = getNextStatement(statements, i + 1);
        if (nextStatement.map(s->!(s instanceof ImportDefinition))
            .orElse(false)) {
          resolveImports(scope);
        }
      }
      return null;
    }

    private void resolveImports(Scope scope) {
      importResolved.set(true);
      ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
      ImportSchema schema = importManager.createImportSchema(errors);
      System.out.println(schema);

      ImportRelationDefinition importRelationDefinition =
        new ImportRelationDefinition(schema);

      analysis.getLogicalPlan()
          .setCurrentDefinition(QualifiedName.of("tableName"), null);
    }

    private Optional<Node> getNextStatement(List<Node> statements, int i) {
      if (i < statements.size()) {
        return Optional.of(statements.get(i));
      }
      return Optional.empty();
    }

    @Override
    protected Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getQualifiedName()));
      }

      if (node.getQualifiedName().getParts().get(node.getQualifiedName().getParts().size() - 1).equalsIgnoreCase("*")) {
        importManager.importAllTable(node.getQualifiedName().getParts().get(0));
      } else {
//        importManager.importTable(node.getQualifiedName().toOriginalString());
      }

      return scope;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      RelationDefinition currentParent = analysis.getLogicalPlan()
          .getCurrentDefinition(queryAssignment.getName().getPrefix().get())
          .orElseThrow(() -> new RuntimeException("Base belation not yet defined"));

      Scope newScope = createAndAssignScope(queryAssignment, null, analysis.getLogicalPlan(), currentParent);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata, this.analysis);
      Scope result = queryAssignment.getQuery().accept(statementAnalyzer, newScope);
      // Obtain the relation header and convert it to an sqml relation
      QueryRelationDefinition queryRelationDefinition = new QueryRelationDefinition(result.getRelation(),
          new RelationIdentifier(queryAssignment.getName()), currentParent);
      if (queryAssignment.getName().getPrefix().isEmpty()) {
        throw new RuntimeException("Query defining base relation not yet implemented");
      } else {
        ExtendedChildQueryRelationDefinition field = new ExtendedChildQueryRelationDefinition(queryAssignment.getName().getSuffix(),
            queryAssignment.getQuery(), currentParent, new QueryField(queryAssignment.getName().getSuffix(), queryRelationDefinition));
        analysis.getLogicalPlan().setCurrentDefinition(queryAssignment.getName(), queryRelationDefinition);
        setRelation(queryAssignment.getName().getPrefix().get(), field);
      }

      return createAndAssignScope(queryAssignment, queryRelationDefinition,
          analysis.getLogicalPlan(), currentParent);
    }

//    private RelationDefinition getRelation(QualifiedName qualifiedName) {
//      return this.analysis.getLogicalPlan().getCurrentDefinition(qualifiedName)
//          .orElseThrow(()->new RuntimeException("Could not find relation"));
//    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        final Scope scope) {
      QualifiedName entityName = expressionAssignment.getName().getPrefix()
          .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", expressionAssignment.getName())));
      RelationDefinition relationDefinition = analysis.getLogicalPlan().getCurrentDefinition(entityName)
          .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", entityName)));

      Scope assignedScope = createAndAssignScope(expressionAssignment.getExpression(),
          relationDefinition, analysis.getLogicalPlan(), relationDefinition);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expressionAssignment.getExpression(), assignedScope);

      Type type = exprAnalysis.getType(expressionAssignment.getExpression());
      Preconditions.checkNotNull(type, "Could not find type for %s %s",
            expressionAssignment.getExpression().getClass().getName(),
            expressionAssignment.getExpression());

      String columnName = expressionAssignment.getName().getSuffix();

      ExtendedFieldRelationDefinition extendedFieldRelationDefinition = new ExtendedFieldRelationDefinition(
          columnName,
          expressionAssignment.getExpression(), relationDefinition,
          Field.newDataField(expressionAssignment.getName().getSuffix(), type));
      setRelation(entityName, extendedFieldRelationDefinition);

      return createAndAssignScope(expressionAssignment.getExpression(),
          extendedFieldRelationDefinition,
          analysis.getLogicalPlan(), relationDefinition);
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription node, Scope context) {
      return null;
    }

    @Override
    public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {
      QualifiedName entityName = QualifiedName.of(node.getTable());
      RelationDefinition shadowedDefinition = analysis.getLogicalPlan().getCurrentDefinition(entityName)
          .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", node.getTable().getValue())));

      DistinctRelationDefinition distinctRelationDefinition = new DistinctRelationDefinition(
          node.getName(),
          node,
          shadowedDefinition,
          new RelationIdentifier(entityName)
      );
      setRelation(entityName, distinctRelationDefinition);

      return scope;
    }

    @Override
    public Scope visitInlineJoin(InlineJoin node, Scope scope) {
      InlineJoinBody join = node.getJoin();

      throw new RuntimeException("Join Declaration TBD");
//      RelationType rel = scope.resolveRelation(join.getTable())
//          .orElseThrow(()-> new RuntimeException(String.format("Could not find relation %s %s", join.getTable(), node)));

//      if (node.getInverse().isPresent()) {
//        RelationType relationType = scope.getRelation();
//        rel.addField(Field.newUnqualified(node.getInverse().get().toString(), relationType));
//      }

      //addRelation(node.getJoin(), rel);
//      return scope;
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      this.analysis.addTypes(analysis.typeMap);

      return analysis;
    }

    private Scope createAndAssignScope(Node node,
        RelationDefinition relationDefinition, LogicalPlan logicalPlan, RelationDefinition current) {
      Scope scope = Scope.builder()
          .withRelationType(relationDefinition)
          .withCurrentSqmlRelation(current)
          .withLogicalPlan(logicalPlan)
          .build();

      analysis.setScope(node, scope);
      return scope;
    }

    private void setRelation(QualifiedName name, RelationDefinition relationDefinition) {
      //1. The parent relation need to be aware of the new column
      //2. Changes of this table should also redefine all of its relational parents...
      // This is because those changes are now visible for new relations defined.
      analysis.getLogicalPlan().setCurrentDefinition(name,
          relationDefinition);

      while (name.getPrefix().isPresent()) {
        name = name.getPrefix().get();
        RelationDefinition previous = analysis.getLogicalPlan().getCurrentDefinition(name)
            .get();
        relationDefinition = new ExtendedChildRelationDefinition(previous);
        analysis.getLogicalPlan().setCurrentDefinition(name,
            relationDefinition);
      }
    }
  }
}
