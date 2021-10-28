package ai.dataeng.sqml.physical;

import ai.dataeng.sqml.ViewQueryRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ColumnNameGen;
import ai.dataeng.sqml.ViewQueryRewriter.DataColumn;
import ai.dataeng.sqml.ViewQueryRewriter.TableExpressionRewriter;
import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.ViewScope;
import ai.dataeng.sqml.ViewQueryRewriter.ViewTable;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.ExpressionAnalysis;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeFormatter;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryAssignment;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.Window;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.flink.table.api.Over;

public class ViewExpressionRewriter extends AstVisitor<ViewScope, ViewRewriterContext> {
  private final PhysicalModel plan;
  private final ColumnNameGen columnNameGen;
  private final Analysis analysis;
  private final ExpressionAnalysis expressionAnalysis;

  public ViewExpressionRewriter(PhysicalModel plan,
      ColumnNameGen columnNameGen, Analysis analysis,
      ExpressionAnalysis expressionAnalysis) {
    this.plan = plan;
    this.columnNameGen = columnNameGen;
    this.analysis = analysis;
    this.expressionAnalysis = expressionAnalysis;
  }

  /**
   * x.y := a - b    => select a - b as y, * from x;
   * x.y := sum(a)   => select sum(a) over (partition by pk), * from x;
   * x.y := sum(z.b) => select sum(z-alias.b) over (partition by pk), * from x
   *                    left outer join z on (join condition) as z-alias;
   * x.y := @.total_output + @.fee;
   */
  public ViewScope rewrite(Expression expression, ViewScope viewScope, QualifiedName name) {
    //Process expression
    // If view scope has a Join requirement, materialize join
    // If view scope has
    Optional<ViewTable> table = plan.getTableByName(name.getPrefix().get());
    String tableName = "a";
    SingleColumn column = new SingleColumn(rewriteExpression(expression, viewScope));
//    List<DataColumn> existingColumns = table.get().getColumns();
    Query query = new Query(
        new QuerySpecification(
          Optional.empty(),
          new Select(false, List.of(
              column
          )),
          buildRelation(table, viewScope),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty()
        ),
        Optional.empty(),
        Optional.empty()
    );

    ViewTable viewTable = new ViewTable(
        name,
        tableName,
        List.of(),
        Optional.of(query)
    );

    System.out.println(
        query.accept(new NodeFormatter(), null)
    );

    return new ViewScope(
        expression,
        viewTable,
        List.of() //columns
    );
  }

  private Expression rewriteExpression(Expression expression, ViewScope viewScope) {
    return ExpressionTreeRewriter.rewriteWith(new TableExpressionRewriter(), expression, viewScope);
  }

  private Relation buildRelation(Optional<ViewTable> table, ViewScope viewScope) {
    return new Table(QualifiedName.of("tbl"));
  }


  public class TableExpressionRewriter
      extends ExpressionRewriter<ViewScope> {

    @Override
    public Expression rewriteFunctionCall(FunctionCall node, ViewScope context,
        ExpressionTreeRewriter<ViewScope> treeRewriter) {
      //If aggregate expression, convert to OVER PARTITION BY PK
      //Else return expression
      SqmlFunction function = expressionAnalysis.getFunction(node);
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

    private List<Expression> partition(ViewScope context) {
      return List.of(new Identifier("pk"));
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, ViewScope context,
        ExpressionTreeRewriter<ViewScope> treeRewriter) {
      //Walk node path.
      //visitSelf
      // -
      //

      return node;
//      Optional<DataColumn> column = context.getTable().getColumn(node.getValue());
//      if (column.isEmpty()) throw new RuntimeException(String.format("Could not find column %s", node.getValue()));
//      return new Identifier(column.get().getPhysicalName()); //todo: new identifier?
    }
  }

  //TODO: @ is an alias to disambiguate global scope and local fields
  //

  class PathWalker {
    public void walk(QualifiedName path) {

    }
  }

  class PathVisitor {
    public void visitSelf() {}
    public void visitParent() {}
    public void visitRelation() {}
    public void visitField() {}
  }

}