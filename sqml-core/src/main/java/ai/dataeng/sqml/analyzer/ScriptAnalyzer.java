package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.model.Model;
import ai.dataeng.sqml.planner.PlanNodeIdAllocator;
import ai.dataeng.sqml.planner.PlanVariableAllocator;
import ai.dataeng.sqml.sql.parser.SqlParser;
import ai.dataeng.sqml.sql.tree.Assign;
import ai.dataeng.sqml.sql.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.Import;
import ai.dataeng.sqml.sql.tree.Script;
import java.util.List;

public class ScriptAnalyzer {

  private final SqlParser sqlParser;
  private final Session session;
  private final List<Expression> parameters;
  private final Metadata metadata;
  private final ImportResolver importResolver = new ImportResolver();
  private final PlanNodeIdAllocator allocator = new PlanNodeIdAllocator();
  private final PlanVariableAllocator variableAllocator = new PlanVariableAllocator();

  public ScriptAnalyzer(Session session,
      SqlParser sqlParser,
      List<Expression> parameters,
      Metadata metadata) {
    this.session = session;
    this.sqlParser = sqlParser;
    this.parameters = parameters;
    this.metadata = metadata;
  }

  public Model analyze(Script script) {
    Model model = new Model();
    new ScriptTraverser(model).process(script, null);
    return model;
  }


  private class ScriptTraverser
      extends DefaultTraversalVisitor<Void, Void> {

    private final Model model;

    public ScriptTraverser(Model model) {
      this.model = model;
    }

    @Override
    protected Void visitImport(Import node, Void context) {
      importResolver.resolve(node, model);
      return null;
    }

    @Override
    protected Void visitAssign(Assign node, Void context) {
//      AnalysisVisitor analysisVisitor = new AnalysisVisitor(model);

//      model.addRelation(node.getName(), node.getRhs().accept(analysisVisitor, new AssignmentContext(null)));
      return null;
    }
  }


//
//  private class AnalysisVisitor
//      extends DefaultTraversalVisitor<Analysis, AssignmentContext> {
//
//    private Model model;
//
//    public AnalysisVisitor(Model model) {
//      this.model = model;
//    }
//
//    @Override
//    public Analysis visitExpressionAssignment(ExpressionAssignment expressionAssignment,
//        AssignmentContext context) {
//      return super.visitExpressionAssignment(expressionAssignment, context);
//    }
//
//    @Override
//    public Analysis visitQueryAssignment(QueryAssignment queryAssignment, AssignmentContext context) {
//      Analyzer analyzer = new Analyzer(session, metadata, sqlParser, List.of());
//      return analyzer.analyze(queryAssignment.getQuery(), context.getName());
//    }
//
//    @Override
//    public Analysis visitRelationshipAssignment(RelationshipAssignment relationshipAssignment,
//        AssignmentContext context) {
//      return super.visitRelationshipAssignment(relationshipAssignment, context);
//    }
//  }
//
//  public static class AssignmentContext {
//
//    private final QualifiedName name;
//
//    public AssignmentContext(QualifiedName name) {
//      this.name = name;
//    }
//
//    public QualifiedName getName() {
//      return name;
//    }
//  }

}
