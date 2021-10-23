package ai.dataeng.sqml;

import ai.dataeng.sqml.ViewQueryRewriter.ViewMaterializerContext;
import ai.dataeng.sqml.ViewQueryRewriter.ViewScope;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.ImportDefinition;
import ai.dataeng.sqml.tree.Node;
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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;

public class ViewQueryRewriter extends AstVisitor<ViewScope, ViewMaterializerContext> {
  public List<ViewTable> tables = new ArrayList<>();

  ColumnNameGen columnNameGen = new ColumnNameGen();
  @Override
  protected ViewScope visitImportDefinition(ImportDefinition node, ViewMaterializerContext context) {
    tables.add(
        new ViewTable(QualifiedName.of("product"),
        "product_1",
        List.of(new DataColumn("productid", "productid_1"),
            new DataColumn(null, "uuid")),
        Optional.empty()
        ));
    return null;
  }

  @Override
  public ViewScope visitQueryAssignment(QueryAssignment node, ViewMaterializerContext context) {
    //todo Create materialization map
    return node.getQuery().accept(this, context);
  }

  @Override
  protected ViewScope visitQuery(Query node, ViewMaterializerContext context) {
    //Todo: reminder of Query node
    return node.getQueryBody().accept(this, context);
  }

  @Override
  protected ViewScope visitQuerySpecification(QuerySpecification node,
      ViewMaterializerContext context) {
    ViewScope fromScope = node.getFrom().accept(this, context);
    Optional<Expression> whereNode = node.getWhere().map(where -> processWhere(node, fromScope, where));

    Select select = processSelect(node, fromScope);

    Optional<GroupBy> groupBy = processGroupBy(node, fromScope, null);

    Optional<Expression> having = processHaving(node, fromScope);

    return new ViewScope(new QuerySpecification(
        node.getLocation(),
        select,
        (Relation)fromScope.node,
        whereNode,
        groupBy,
        having,
        Optional.empty(), //order by
        Optional.empty()// limit
    ), null);
  }

  private Optional<Expression> processHaving(QuerySpecification node, ViewScope fromScope) {
    if (node.getHaving().isPresent()) {
      Expression having = node.getHaving().get();
      return Optional.of(rewriteExpression(having, fromScope));
    }
    return Optional.empty();
  }

  private Optional<GroupBy> processGroupBy(QuerySpecification node, ViewScope fromScope,
      //Output expressions are referenced expressions in the select clause (e.g. select x + 1 as y ... group by y;)
      List<Expression> outputExpressions) {
    if (node.getGroupBy().isEmpty()) return Optional.empty();
    GroupBy groupBy = node.getGroupBy().get();
    GroupingElement groupingElement = groupBy.getGroupingElement();

    //TODO: From view scope, get current context keys

    List<Expression> expressions = new ArrayList<>();
    for (Expression expression : groupingElement.getExpressions()) {
      Expression rewritten = rewriteExpression(expression, fromScope);
      expressions.add(rewritten);
    }

    return Optional.of(new GroupBy(new SimpleGroupBy(expressions)));
  }

  private Expression processWhere(QuerySpecification node, ViewScope fromScope, Expression where) {
    return rewriteExpression(where, fromScope);
  }

  private Select processSelect(QuerySpecification node, ViewScope from) {
    List<SelectItem> columns = new ArrayList<>();
    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn) {
        //todo: Can we get this from the analysis?
        SingleColumn column = (SingleColumn) item;

        //Todo: tracking aliasing ?
        Expression rewritten = rewriteExpression(column.getExpression(), from);
        String physicalName = columnNameGen.generateName(column.getAlias().get().getValue());
        columns.add(new SingleColumn(rewritten,
            new Identifier(physicalName)));
        //Todo: Mapping from alias or name to column name

//        from.addMapping(physicalName, column.getAlias().get().getValue());
      } else {
        throw new RuntimeException("tbd");
      }
    }

    //

    return new Select(node.getSelect().isDistinct(), columns);
  }

  private Expression rewriteExpression(Expression expression, ViewScope viewScope) {
    return ExpressionTreeRewriter.rewriteWith(new TableExpressionRewriter(), expression, viewScope);
  }

  @Override
  protected ViewScope visitTable(Table node, ViewMaterializerContext context) {
    Preconditions.checkState(node.getName().getParts().size() == 1, "Table paths tbd");
    Optional<ViewTable> table = getTable(node.getName());
    Preconditions.checkState(table.isPresent(), "Could not find table %s", node.getName());

    return new ViewScope(
        new Table(QualifiedName.of(table.get().getTableName())),
        table.get()
      );
  }

  private Optional<ViewTable> getTable(QualifiedName name) {
    List<ViewTable> viewTables = this.tables;
    for (int i = viewTables.size() - 1; i >= 0; i--) {
      ViewTable table = viewTables.get(i);
      if (table.getPath().equals(name)) {
        return Optional.of(table);
      }
    }

    return Optional.empty();
  }

  public class TableExpressionRewriter
      extends ExpressionRewriter<ViewScope> {

    @Override
    public Expression rewriteIdentifier(Identifier node, ViewScope context,
        ExpressionTreeRewriter<ViewScope> treeRewriter) {
            Optional<DataColumn> column = context.getTable().getColumn(node.getValue());
      if (column.isEmpty()) throw new RuntimeException(String.format("Could not find column %s", node.getValue()));
      return new Identifier(column.get().getPhysicalName()); //todo: new identifier?
    }
  }

  @Value
  public static class ViewScope {
    Node node;
    ViewTable table;

    public String getTableName() {
      return null;
    }

    public List<DataColumn> getColumns() {
      return null;
    }
  }

  @Value
  public static class ViewMaterializerContext {
    StatementAnalysis analysis;
    Scope result;
    ImportSchema schema;
  }

  @Value
  public static class ViewTable {
    QualifiedName path;

    String tableName;
    List<DataColumn> columns;

    Optional<Node> queryAst;

    public List<DataColumn> getColumns() {
      Map<String, DataColumn> cols = columns.stream().collect(
          Collectors.toUnmodifiableMap(t -> t.getLogicalName(), Function.identity(),
              (v1, v2) -> v2));
      return new ArrayList<>(cols.values());
    }

    public Optional<DataColumn> getColumn(String value) {
      List<DataColumn> selectionSet = columns;
      for (int i = selectionSet.size() - 1; i >= 0; i--) {
        DataColumn column = selectionSet.get(i);
        if (column.getLogicalName() != null && column.getLogicalName().equals(value)) {
          return Optional.of(column);
        }
      }
      return Optional.empty();
    }

  }

  public interface Column {

  }

  @Value
  public static class DataColumn implements Column {
    String logicalName;
    String physicalName;
  }

  public class ColumnNameGen {
    int count = 0;
    public String generateName(String name) {
      return name + "_" + (++count);
    }
  }
}