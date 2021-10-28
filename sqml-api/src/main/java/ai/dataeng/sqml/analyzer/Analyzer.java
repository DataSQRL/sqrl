package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.ViewQueryRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ColumnNameGen;
import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.RewriterContext;
import ai.dataeng.sqml.execution.importer.ImportManager;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.ingest.schema.SchemaConversionError;
import ai.dataeng.sqml.logical3.ExpressionField;
import ai.dataeng.sqml.logical3.LogicalPlan;
import ai.dataeng.sqml.logical3.ParentField;
import ai.dataeng.sqml.logical3.QueryRelationField;
import ai.dataeng.sqml.logical3.RelationshipField;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.physical.ViewExpressionRewriter;
import ai.dataeng.sqml.schema2.Field;
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
import com.google.common.base.Preconditions;
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
        //add parent fields
        addParentFields(field, Optional.empty());
        analysis.getPlanBuilder().getRoot().add(field);
      }

      createPhysicalView(node, scope, Optional.of(schema), Optional.empty(), Optional.empty());

      return scope;
    }

    @Override
    public Scope visitQueryAssignment(QueryAssignment queryAssignment, Scope scope) {
      QualifiedName name = queryAssignment.getName();
      Query query = queryAssignment.getQuery();

      Optional<TypedField> prefixField = getPrefixField(queryAssignment);
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(this.metadata,
          this.analysis.getPlanBuilder());
      Scope result = query.accept(statementAnalyzer, Scope.create(prefixField));

      QueryRelationField createdField = new QueryRelationField(name.getSuffix(), result.getRelation());
      prefixField.ifPresent(f->addParentField(f, result.getRelation()));
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

      Scope exprScope = Scope.create((RelationType)unbox(field.getType()), fieldOptional);
      ExpressionAnalysis exprAnalysis = analyzeExpression(expression, exprScope);
      Type type = exprAnalysis.getType(expression);

      TypedField createdField = new ExpressionField(name.getSuffix(), type, Optional.empty());
      addField(expressionAssignment, createdField);

      createPhysicalExpression(expressionAssignment, exprScope,
          exprAnalysis);

      return null;
    }

    @Override
    public Scope visitCreateSubscription(CreateSubscription subscription, Scope scope) {
      StatementAnalyzer statementAnalyzer = new StatementAnalyzer(metadata, this.analysis.getPlanBuilder());
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
      Optional<TypedField> field = analysis.getPlanBuilder()
          .getField(node.getName().getPrefix());

      TypedField to = analysis.getPlanBuilder().resolveTableField(table, field)
          .orElseThrow(/*todo throw table must exist*/);

      RelationshipField createdField = new RelationshipField(node.getName().getSuffix(), to);
      addField(node, createdField);

      Optional<Identifier> identifier = node.getInlineJoin().getInverse();
      if (identifier.isPresent()) {
        QualifiedName name = QualifiedName.of(identifier.get());
        TypedField field1 = field.get();
        RelationshipField inverseField = new RelationshipField(name.getSuffix(), field1);
        analysis.getPlanBuilder().addField(to.getQualifiedName(), inverseField);
      }

      return null;
    }

    @Override
    public Scope visitInlineJoin(InlineJoin node, Scope context) {
      //Todo: inline join assignment
      return context;
    }

    private void addParentField(TypedField f, RelationType<TypedField> relation) {
      Preconditions.checkState(unbox(f.getType()) instanceof RelationType);
      relation.add(new ParentField(f));
    }

    private void addField(Assignment node, TypedField createdField) {
      analysis.getPlanBuilder().addFieldPrefix(node.getName(), createdField);
    }

    private void addParentFields(StandardField field, Optional<TypedField> parent) {
      Type type = unbox(field.getType());
      if (!(type instanceof RelationType)) {
        return;
      }
      RelationType<Field> rel = (RelationType<Field>) type;
      for (Field sField : rel.getFields()) {
        if (sField instanceof StandardField) {
          addParentFields((StandardField)sField, Optional.of(field));
        }
      }

      parent.ifPresent(f -> rel.add(new ParentField(f)));
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope) {
      ExpressionAnalyzer analyzer = new ExpressionAnalyzer(metadata, this.analysis.getPlanBuilder());
      ExpressionAnalysis analysis = analyzer.analyze(expression, scope);

      return analysis;
    }

    private Optional<TypedField> getPrefixField(Assignment assignment) {
      return analysis.getPlanBuilder().getField(assignment.getName().getPrefix());
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
        RewriterContext rewriterContext = new RewriterContext(node, null, null);
        RewriterContext scope2 = viewRewriter.rewrite(node.getExpression(), rewriterContext, node.getName(), scope);

        this.analysis.getPhysicalModel()
            .addTable(scope2.getTable());
      } catch (Exception e) {
        log.error("Physical plan err");
      }
    }

    private void createPhysicalView(Node node, Scope scope,
        Optional<ImportSchema> schema, Optional<StatementAnalysis> analysis,
        Optional<ExpressionAnalysis> expressionAnalysis) {
      try {
        ViewQueryRewriter viewRewriter = new ViewQueryRewriter(this.analysis.getPhysicalModel(),
            columnNameGen);
        RewriterContext scope2 = node.accept(viewRewriter, new ViewRewriterContext(scope, analysis,
            schema, expressionAnalysis));
        if (scope2 == null) {
          log.error("Err, no scope");
          return;
        }
        if (true) {
          return;
        }
        System.out.println(scope2.getTable().getQueryAst().get().accept(new NodeFormatter(), null));

      } catch (Exception e) {
        log.error("Physical plan err");
//        e.printStackTrace();
      }
    }
  }
}
