package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.ViewQueryRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ColumnNameGen;
import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.ViewScope;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.logical3.ExpressionField;
import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.logical3.QueryRelationField;
import ai.dataeng.sqml.logical3.RelationshipField;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.physical.ViewExpressionRewriter;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.basic.ConversionError;
import ai.dataeng.sqml.tree.Assignment;
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
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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
    Analysis analysis = new Analysis(script, new PhysicalModel(), new LogicalPlan.Builder());
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
        StandardField field = schema.getSchema().getFieldByName(mapping.getKey());

        analysis.getPlanBuilder().getRoot().add(field);
      }

      createPhysicalView(node, scope, Optional.of(schema), Optional.empty(), Optional.empty());

      return scope;
    }
//
//    private LogicalField decorateRelation(StandardField field, Optional<RelationType> parent) {
//      Type type = decorateRelation(field.getType());
//      LogicalField f = new LogicalPlan.DataField(field.getName(),
//          type, field.getConstraints());
//
//      Type unboxed = unbox(type);
//      if (unboxed instanceof RelationType && parent.isPresent()) {
//        RelationType rel = (RelationType) unboxed;
//        rel.add(new ParentField(parent.get()));
//      }
//
//      return f;
//    }
//
//    private Type decorateRelation(Type type) {
//      SqmlTypeVisitor<Type, Type> sqmlTypeVisitor = new SqmlTypeVisitor<Type, Type>() {
//        @Override
//        public <F extends Field> Type visitRelation(RelationType<F> relationType, Type context) {
//          RelationType<LogicalField> rel = new RelationType<>();
//          for (StandardField field : (List<StandardField>) relationType.getFields()) {
//            rel.add(decorateRelation(field, Optional.of(rel)));
//          }
//
//          return rel;
//        }
//
//        @Override
//        public Type visitType(Type type, Type context) {
//          return type;
//        }
//
//        @Override
//        public Type visitArrayType(ArrayType type, Type context) {
//          return new ArrayType(type.getSubType().accept(this, context));
//        }
//      };
//
//      return type.accept(sqmlTypeVisitor, null);
//    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      QualifiedName name = queryAssignment.getName();
      Query query = queryAssignment.getQuery();

      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata);
      Scope result = query.accept(statementAnalyzer, Scope.create(getPrefixField(queryAssignment)));

      QueryRelationField createdField = new QueryRelationField(name.getSuffix(), result.getRelation());
      addField(queryAssignment, createdField);

      createPhysicalView(queryAssignment, result, Optional.empty(), Optional.of(statementAnalyzer.getAnalysis()),
          Optional.empty());

      return null;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        final Scope scope) {
      QualifiedName name = expressionAssignment.getName();
      Expression expression = expressionAssignment.getExpression();

      Optional<TypedField> fieldOptional = getPrefixField(expressionAssignment);
      TypedField field = fieldOptional
          .orElseThrow(/* expression must be assigned to relation */);

      if (!(unbox(field.getType()) instanceof RelationType)) {
        throw new RuntimeException(
            String.format("Can only extend a relation with an expression. Found: %s ",
                unbox(field.getType()).getClass().getName()));
      }

      ExpressionAnalysis exprAnalysis = analyzeExpression(expression, Scope.create(fieldOptional));
      Type type = exprAnalysis.getType(expression);

      TypedField createdField = new ExpressionField(name.getSuffix(), type, Optional.empty());
      addField(expressionAssignment, createdField);

      createPhysicalExpression(expressionAssignment, null,
          exprAnalysis);

      return null;
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription subscription, Scope scope) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata);
      Scope queryScope = subscription.getQuery().accept(statementAnalyzer, scope);

      return null;
    }

    @Override
    public Scope visitDistinctAssignment(DistinctAssignment node, Scope scope) {

      return null;
    }

    @Override
    public Scope visitJoinAssignment(JoinAssignment node, Scope scope) {
      QualifiedName table = node.getInlineJoin().getJoin().getTable();
      TypedField to = analysis.getPlanBuilder().getField(table)
          .orElseThrow(/*todo throw table must exist*/);

      RelationshipField createdField = new RelationshipField(node.getName().getSuffix(), to);
      addField(node, createdField);

      return null;
    }

    private void addField(Assignment node, TypedField createdField) {
      analysis.getPlanBuilder().addField(node.getName(), createdField);
    }

    @Override
    public Scope visitInlineJoin(InlineJoin node, Scope context) {
      //Todo: inline join assignment
      return context;
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata);
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      return analysis;
    }

    private Optional<TypedField> getPrefixField(Assignment assignment) {
      Optional<TypedField> field = assignment.getName().getPrefix()
          .map(p -> this.analysis.getPlanBuilder().getField(p))
          .orElseThrow(/*todo*/);
      return field;
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

    private void createPhysicalExpression(ExpressionAssignment node, Scope scope,
        ExpressionAnalysis expressionAnalysis) {
      try {
        ViewExpressionRewriter viewRewriter = new ViewExpressionRewriter(this.analysis.getPhysicalModel(),
            columnNameGen, this.analysis, expressionAnalysis);
        ViewScope viewScope = new ViewScope(node, null, null);
        ViewScope scope2 = viewRewriter.rewrite(node.getExpression(), viewScope, node.getName());

        this.analysis.getPhysicalModel()
            .addTable(scope2.getTable());
      } catch (Exception e) {
        log.error("Physical plan err");
        e.printStackTrace();
      }
    }

    private void createPhysicalView(Node node, Scope scope,
        Optional<ImportSchema> schema, Optional<StatementAnalysis> analysis,
        Optional<ExpressionAnalysis> expressionAnalysis) {
      try {
        ViewQueryRewriter viewRewriter = new ViewQueryRewriter(this.analysis.getPhysicalModel(),
            columnNameGen);
        ViewScope scope2 = node.accept(viewRewriter, new ViewRewriterContext(scope, analysis,
            schema, expressionAnalysis));
        if (scope2 == null) {
          log.error("Err, no scope");
          return;
        }
        if (true) {
          return;
        }
        System.out.println(scope2.getTable().getQueryAst().get().accept(new NodeFormatter(), null));
        this.analysis.getPhysicalModel()
            .addTable(scope2.getTable());
      } catch (Exception e) {
        log.error("Physical plan err");
        e.printStackTrace();
      }
    }
  }
}
