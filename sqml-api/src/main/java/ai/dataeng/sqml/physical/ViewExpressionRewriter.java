package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.ViewQueryRewriter.ColumnNameGen;
import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.RewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.ViewTable;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.ExpressionAnalysis;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.Window;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ViewExpressionRewriter extends AstVisitor<RewriterContext, ViewRewriterContext> {
  private final PhysicalModel plan;
  private final ColumnNameGen columnNameGen;
  private final Analysis analysis;
  private final ExpressionAnalysis exprAnalysis;

  public ViewExpressionRewriter(PhysicalModel plan,
      ColumnNameGen columnNameGen, Analysis analysis,
      ExpressionAnalysis expressionAnalysis) {
    this.plan = plan;
    this.columnNameGen = columnNameGen;
    this.analysis = analysis;
    this.exprAnalysis = expressionAnalysis;
  }

  /**
   * x.y := a - b    => select a - b as y, * from x;
   * x.y := sum(a)   => select sum(a) over (partition by pk), * from x;
   * x.y := sum(z.b) => select sum(zalias.b) over (partition by pk), * from x
   *                    left outer join z on (join condition) as z-alias;
   * x.y := @.total_output + @.fee;
   */
  public RewriterContext rewrite(Expression expression, RewriterContext viewScope, QualifiedName name,
      Scope scope) {

    return null;
  }

  private Expression rewriteExpression(Expression expression, RewriterContext rewriterContext) {
    return ExpressionTreeRewriter.rewriteWith(new TableExpressionRewriter(), expression,
        rewriterContext);
  }

  private Relation buildRelation(Scope scope) {
    TypedField field = scope.getField().orElseThrow(/*expression must be on query*/);

//    List<FieldPath> fieldPaths = expressionAnalysis.getFieldPaths();
//    FieldTree fieldTree = new FieldTree(fieldPaths);

    ViewTable table = plan.getTableByName(field.getQualifiedName())
        .orElseThrow(/*No physical table found for path*/);

    return new Table(QualifiedName.of(table.getTableName()));
  }


  public class TableExpressionRewriter
      extends ExpressionRewriter<RewriterContext> {

    @Override
    public Expression rewriteFunctionCall(FunctionCall node, RewriterContext context,
        ExpressionTreeRewriter<RewriterContext> treeRewriter) {
      //If aggregate expression, convert to OVER PARTITION BY PK
      //Else return expression
      SqmlFunction function = exprAnalysis.getFunction(node);
      if (function.isAggregation()) {

        return new FunctionCall(
            Optional.empty(),
            QualifiedName.of(function.getName()),
            node.getArguments().stream()
                .map(a->treeRewriter.rewrite(a, context))
                .collect(Collectors.toList()),
            node.isDistinct(),
            Optional.of(new Window(partition(context), Optional.empty()))
          );
      } else {
        return node;
      }
    }

    private List<Expression> partition(RewriterContext context) {
      return List.of(new Identifier("pk"));
    }

    /**
     * Identifier could be:
     *  field
     *  rel.field
     *  *
     */
    @Override
    public Expression rewriteIdentifier(Identifier node, RewriterContext context,
        ExpressionTreeRewriter<RewriterContext> treeRewriter) {
      return node;
    }
  }
}