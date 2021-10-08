package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.logical.DataField;
import ai.dataeng.sqml.logical.DistinctRelationDefinition;
import ai.dataeng.sqml.logical.ImportRelationDefinition;
import ai.dataeng.sqml.logical.InlineJoinFieldDefinition;
import ai.dataeng.sqml.logical.RelationIdentifier;
import ai.dataeng.sqml.logical.ExtendedChildQueryRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedFieldRelationDefinition;
import ai.dataeng.sqml.logical.ExtendedChildRelationDefinition;
import ai.dataeng.sqml.logical.QueryField;
import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.logical.SubscriptionDefinition;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.schema2.ArrayType;
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
import ai.dataeng.sqml.tree.InlineJoinBody;
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
    private final ImportManager importManager;

    public Visitor(Analysis analysis, Metadata metadata) {
      this.analysis = analysis;
      this.metadata = metadata;
      this.importManager = new ImportManager(metadata.getEnv()
          .getDdRegistry());
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
        completeImportHeader(statements, i, scope);
      }

      analysis.setLogicalPlan(scope.getLogicalPlanBuilder().build());

      return null;
    }

    @Override
    protected Scope visitImportDefinition(ImportDefinition node, Scope scope) {
      if (importResolved.get()) {
        throw new RuntimeException(String.format("Import statement must be in header %s", node.getQualifiedName()));
      }

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

      return scope;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      QualifiedName name = queryAssignment.getName();
      if (name.getPrefix().isEmpty()) {
        return visitBaseQueryAssignment(queryAssignment, scope);
      }
      QualifiedName namePrefix = name.getPrefix().get();
      RelationDefinition currentParent = scope
            .getCurrentDefinition(namePrefix)
            .orElseThrow(() -> new RuntimeException("Base relation not yet defined"));

      Query query = queryAssignment.getQuery();
      Scope newScope = createAndAssignScope(query, null, Optional.of(currentParent), scope);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata);
      Scope result = query.accept(statementAnalyzer, newScope);

      QueryRelationDefinition queryRelationDefinition = new QueryRelationDefinition(result.getRelation(),
          new RelationIdentifier(name), Optional.of(currentParent), scope, statementAnalyzer.getAnalysis()
          .getReferencedRelations());
      scope.setCurrentDefinition(name, queryRelationDefinition);

      ExtendedChildQueryRelationDefinition field = new ExtendedChildQueryRelationDefinition(name.getSuffix(),
          queryAssignment.getQuery(), currentParent, new QueryField(name.getSuffix(), queryRelationDefinition));
      scope.setCurrentDefinition(namePrefix, field);

      notifyAncestors(namePrefix, scope);

      return createAndAssignScope(queryAssignment, queryRelationDefinition,
          Optional.of(currentParent), scope);
    }

    private Scope visitBaseQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      QualifiedName name = queryAssignment.getName();
      Query query = queryAssignment.getQuery();
      Scope newScope = createAndAssignScope(query, null, Optional.empty(), scope);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata);
      Scope result = query.accept(statementAnalyzer, newScope);

      QueryRelationDefinition queryRelationDefinition = new QueryRelationDefinition(result.getRelation(),
          new RelationIdentifier(name),
          Optional.empty(), scope, statementAnalyzer.getAnalysis().getReferencedRelations());
      scope.setCurrentDefinition(name, queryRelationDefinition);

      return createAndAssignScope(queryAssignment, queryRelationDefinition,
          Optional.empty(), scope);
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        final Scope scope) {
      QualifiedName name = expressionAssignment.getName();
      QualifiedName prefixName = name.getPrefix()
          .orElseThrow(()->new RuntimeException(String.format("Cannot assign expression as a base relation %s", name)));
      RelationDefinition relationDefinition = scope.getCurrentDefinition(prefixName)
          .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", prefixName)));

      Expression expression = expressionAssignment.getExpression();

      Scope assignedScope = createAndAssignScope(expression,
          relationDefinition, Optional.of(relationDefinition), scope);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expression, assignedScope);

      Type type = exprAnalysis.getType(expression);

      String fieldName = name.getSuffix();
      ExtendedFieldRelationDefinition extendedFieldRelationDefinition = new ExtendedFieldRelationDefinition(
          fieldName,
          expression, relationDefinition,
          DataField.newDataField(name.getSuffix(), type));

      scope.setCurrentDefinition(prefixName, extendedFieldRelationDefinition);
      notifyAncestors(prefixName, scope);

      return createAndAssignScope(expression,
          extendedFieldRelationDefinition,
          Optional.of(extendedFieldRelationDefinition), scope);
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription subscription, Scope scope) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata);
      Scope queryScope = subscription.getQuery().accept(statementAnalyzer, scope);

      SubscriptionDefinition subscriptionDefinition = new SubscriptionDefinition(subscription,
          queryScope);
      scope.setSubscriptionDefinition(subscriptionDefinition);

      return createAndAssignScope(subscription,
          subscriptionDefinition,
          Optional.of(subscriptionDefinition), scope);
    }

    @Override
    public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {
      QualifiedName tableName = QualifiedName.of(node.getTable());
      RelationDefinition shadowedDefinition = scope.getCurrentDefinition(tableName)
          .orElseThrow(()->new RuntimeException(String.format("Could not find relation %s", node.getTable().getValue())));

      DistinctRelationDefinition distinctRelationDefinition = new DistinctRelationDefinition(
          node.getName(),
          node,
          shadowedDefinition,
          new RelationIdentifier(tableName)
      );
      scope.setCurrentDefinition(tableName, distinctRelationDefinition);

      notifyAncestors(tableName, scope);

      return scope;
    }

    @Override
    public Scope visitInlineJoin(InlineJoin node, Scope scope) {
      QualifiedName name = QualifiedName.of("");
      InlineJoinBody join = node.getJoin();
      InlineJoinFieldDefinition inlineJoinFieldDefinition = new InlineJoinFieldDefinition();

      RelationDefinition relationDefinition = scope.getCurrentDefinition(name)
          .orElseThrow(()->new RuntimeException("Could not find relation for inline join"));

      ExtendedFieldRelationDefinition extendedFieldRelationDefinition = new ExtendedFieldRelationDefinition(name.getSuffix(), node, relationDefinition,
          inlineJoinFieldDefinition);
      scope.setCurrentDefinition(name, extendedFieldRelationDefinition);

      if (node.getInverse().isPresent()) {
        RelationType relationType = scope.getRelation();
        Identifier identifier = node.getInverse().get();
        RelationDefinition inverse = scope.getCurrentDefinition(QualifiedName.of(identifier))
            .orElseThrow(()->new RuntimeException("Could not find inverse relation for inline join"));

        ExtendedFieldRelationDefinition inverseField = new ExtendedFieldRelationDefinition(identifier.getValue(), node, inverse,
            DataField.newDataField(Name.of(identifier.getValue(), NameCanonicalizer.SYSTEM), inverse));

        scope.setCurrentDefinition(inverse.getPath(), inverseField);
      }

      return createAndAssignScope(node, extendedFieldRelationDefinition, Optional.empty(), scope);
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      this.analysis.addTypes(analysis.typeMap);

      return analysis;
    }

    private Scope createAndAssignScope(Node node,
        RelationDefinition relationDefinition, Optional<RelationDefinition> current,
        Scope parentScope) {
      Scope scope = Scope.builder()
          .withParent(parentScope)
          .withRelationType(relationDefinition)
          .withCurrentSqmlRelation(current)
          .build();

      analysis.setScope(node, scope);
      return scope;
    }

    private void notifyAncestors(QualifiedName name, Scope scope) {
      RelationDefinition relationDefinition;

      while (name.getPrefix().isPresent()) {
        name = name.getPrefix().get();
        RelationDefinition previous = scope.getCurrentDefinition(name)
            .get();
        relationDefinition = new ExtendedChildRelationDefinition(previous);
        scope.setCurrentDefinition(name,
            relationDefinition);
      }
    }

    private void completeImportHeader(List<Node> statements,
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
      ConversionError.Bundle<SchemaConversionError> errors = new ConversionError.Bundle<>();
      ImportSchema schema = importManager.createImportSchema(errors);
      for (Map.Entry<Name, Mapping> mapping : schema.getMappings().entrySet()) {
        QualifiedName name = QualifiedName.of(mapping.getKey().getCanonical());

        ImportRelationDefinition importRelationDefinition =
            new ImportRelationDefinition(mapping.getValue().getDatasetName(),
                mapping.getValue().getType(),
                mapping.getValue().getTableName(),
                name,
                //Todo: remove schema amalgam
                (RelationType)((ArrayType)schema.getSchema().getFieldByName(mapping.getKey()).getType()).getSubType());

        scope.setCurrentDefinition(name, importRelationDefinition);
      }

    }

    private Optional<Node> getNextStatement(List<Node> statements, int i) {
      if (i < statements.size()) {
        return Optional.of(statements.get(i));
      }
      return Optional.empty();
    }

  }
}
