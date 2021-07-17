package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.Assign;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionAssignment;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Import;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import java.util.Optional;

public class Analyzer {
  private final Metadata metadata;
  private final Analysis analysis;
  private final Script script;

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
    TypeInferenceVisitor typeInferenceVisitor = new TypeInferenceVisitor();
    script.accept(typeInferenceVisitor, null);



    Visitor visitor = new Visitor();
    script.accept(visitor, Optional.empty());
  }

  class TypeScope {

  }
  class TypeInferenceVisitor extends DefaultTraversalVisitor<TypeScope, TypeScope> {

    @Override
    public TypeScope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        TypeScope context) {
      TypeScope scope = new TypeScope();
      expressionAssignment.getExpression().accept(this, scope);

      return super.visitExpressionAssignment(expressionAssignment, context);
    }

    @Override
    protected TypeScope visitArithmeticBinary(ArithmeticBinaryExpression node, TypeScope context) {
      System.out.println("");
      return super.visitArithmeticBinary(node, context);
    }

    @Override
    protected TypeScope visitArithmeticUnary(ArithmeticUnaryExpression node, TypeScope context) {
      return super.visitArithmeticUnary(node, context);
    }

  }

  class Scope {
    private final QualifiedName path;
    private final Optional<SqmlType> type;

    public Scope(QualifiedName path) {
      this.path = path;
      this.type = Optional.empty();
    }

    public Scope(QualifiedName path, SqmlType type) {
      this.path = path;
      this.type = Optional.of(type);
    }

    public QualifiedName getPath() {
      return path;
    }

    public Optional<SqmlType> getType() {
      return type;
    }
  }

  class Visitor extends DefaultTraversalVisitor<Scope, Optional<Scope>> {

    @Override
    protected Scope visitAssign(Assign node, Optional<Scope> context) {
      Scope scope = new Scope(node.getName());
      node.getRhs().accept(this, Optional.of(scope));
      return null;
    }

    @Override
    public Scope visitExpressionAssignment(ExpressionAssignment expressionAssignment,
        Optional<Scope> context) {
      Scope scope = expressionAssignment.getExpression().accept(this, context);
      if (scope != null) {
        scope.getType().ifPresent(t -> analysis.getTypeMap().put(scope.getPath(), t));
      }
      return null;
    }

    @Override
    protected Scope visitImport(Import node, Optional<Scope> context) {
      return super.visitImport(node, context);
    }

    @Override
    protected Scope visitExpression(Expression node, Optional<Scope> context) {
      return super.visitExpression(node, context);
    }

    @Override
    protected Scope visitIdentifier(Identifier node, Optional<Scope> context) {
      return new Scope(context.orElseThrow().getPath(), SqmlType.of(node.getTypeHint()));
    }
  }
}
