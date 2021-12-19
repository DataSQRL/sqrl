package ai.dataeng.sqml;

import ai.dataeng.sqml.ViewQueryRewriter.ViewRewriterContext;
import ai.dataeng.sqml.ViewQueryRewriter.RewriterContext;
import ai.dataeng.sqml.analyzer.ExpressionAnalysis;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.execution.importer.ImportSchema;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.physical.PhysicalModel;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
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
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NameCanonicalizer;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;

public class ViewQueryRewriter extends AstVisitor<RewriterContext, ViewRewriterContext> {
  private final PhysicalModel plan;
  private final ColumnNameGen columnNameGen;

  public ViewQueryRewriter(PhysicalModel plan,
      ColumnNameGen columnNameGen) {
    this.plan = plan;
    this.columnNameGen = columnNameGen;
  }
  @Override
  public RewriterContext visitImportDefinition(ImportDefinition node, ViewRewriterContext context) {
    ImportSchema schema = context.getSchema().get();
    for (Map.Entry<Name, Mapping> mapping : schema.getMappings().entrySet()) {
      StandardField field = schema.getSchema().getFieldByName(mapping.getKey());
      if (field.getName().getCanonical().equalsIgnoreCase("product")) {
        ViewTable table = new ViewTable(QualifiedName.of("product"),
            "product_1",
            List.of(
                new DataColumn("productid", "productid_1"),
                new DataColumn("name", "name_1"),
                new DataColumn("description", "description_1"),
                new DataColumn("category", "category_1"),
                new DataColumn(null, "uuid")
            ),
            Optional.empty()
        );
        plan.addTable(table);
        return new RewriterContext(node, table, table.getColumns());
      } else if (field.getName().getCanonical().equalsIgnoreCase("orders")) {
        ViewTable table = new ViewTable(QualifiedName.of("orders"),
            "orders_1",
            List.of(
                new DataColumn("customerid", "customerid_1"),
                new DataColumn("id", "id_1"),
                new DataColumn("time", "time_1"),
                new DataColumn(null, "uuid")
            ),
            Optional.empty()
        );
        plan.addTable(table);
        ViewTable entries = new ViewTable(QualifiedName.of("orders","entries"),
            "orders_entries_1",
            List.of(
                new DataColumn("productid", "productid_1"),
                new DataColumn("quantity", "quantity_1"),
                new DataColumn("unit_price", "unit_price_1"),
                new DataColumn("discount", "discount_1"),
                new DataColumn(null, "uuid")
            ),
            Optional.empty()
        );
        plan.addTable(entries);
      } else if (field.getName().getCanonical().equalsIgnoreCase("customer")) {
        ViewTable table = new ViewTable(QualifiedName.of("customer"),
            "customer_1",
            List.of(
                new DataColumn("customerid", "customerid_1"),
                new DataColumn("name", "name_1"),
                new DataColumn("email", "email_1"),
                new DataColumn(null, "uuid")
            ),
            Optional.empty()
        );
        plan.addTable(table);
      }

    }

    return null;
  }

  @Override
  public RewriterContext visitQueryAssignment(QueryAssignment node, ViewRewriterContext context) {
    //todo Create materialization map
    RewriterContext rewritten = node.getQuery().accept(this, context);
    ViewTable viewTable = new ViewTable(
        node.getName(),
        columnNameGen.generateName(node.getName().getParts().get(0)),
        rewritten.getColumns(),
        Optional.of((Query)rewritten.node));

    return new RewriterContext(node, viewTable, rewritten.getColumns());
  }

  @Override
  public RewriterContext visitQuery(Query node, ViewRewriterContext context) {
    //Todo: reminder of Query node
    return node.getQueryBody().accept(this, context);
  }

  @Override
  public RewriterContext visitQuerySpecification(QuerySpecification node,
      ViewRewriterContext context) {
    RewriterContext fromScope = node.getFrom().accept(this, context);
    Optional<Expression> whereNode = node.getWhere().map(where -> processWhere(node, fromScope, where));

    SelectResults select = processSelect(node, fromScope);

    Optional<GroupBy> groupBy = processGroupBy(node, fromScope, null);

    Optional<Expression> having = processHaving(node, fromScope);

    return new RewriterContext(new QuerySpecification(
        node.getLocation(),
        select.getSelect(),
        (Relation)fromScope.node,
        whereNode,
        groupBy,
        having,
        Optional.empty(), //order by
        Optional.empty()// limit
    ), null, select.getColumns());
  }

  private Optional<Expression> processHaving(QuerySpecification node, RewriterContext fromScope) {
    if (node.getHaving().isPresent()) {
      Expression having = node.getHaving().get();
      return Optional.of(rewriteExpression(having, fromScope));
    }
    return Optional.empty();
  }

  private Optional<GroupBy> processGroupBy(QuerySpecification node, RewriterContext fromScope,
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

  private Expression processWhere(QuerySpecification node, RewriterContext fromScope, Expression where) {
    return rewriteExpression(where, fromScope);
  }

  private SelectResults processSelect(QuerySpecification node, RewriterContext from) {
    List<DataColumn> dataColumns = new ArrayList<>();
    List<SelectItem> columns = new ArrayList<>();
    for (SelectItem item : node.getSelect().getSelectItems()) {
      if (item instanceof SingleColumn) {
        //todo: Can we get this from the analysis?
        SingleColumn column = (SingleColumn) item;

        //Todo: tracking aliasing ?
        Expression rewritten = rewriteExpression(column.getExpression(), from);
        String physicalName = columnNameGen.generateName(column);
        columns.add(new SingleColumn(rewritten,
            new Identifier(physicalName)));

        dataColumns.add(new DataColumn(extractColumnName(column), physicalName));
      } else {
        throw new RuntimeException("tbd");
      }
    }

    return new SelectResults(new Select(node.getSelect().isDistinct(), columns),
        dataColumns);
  }

  private String extractColumnName(SingleColumn name) {
    if (name.getAlias().isPresent()) {
      return name.getAlias().get().getValue() + "_" + (++columnNameGen.count);
    } else if (name.getExpression() instanceof Identifier) {
      return ((Identifier)name.getExpression()).getValue() + "_" + (++columnNameGen.count);
    }

    return "VAR_" + (++columnNameGen.count);
  }

  @Value
  class SelectResults {
    Select select;
    List<DataColumn> columns;
  }

  private Expression rewriteExpression(Expression expression, RewriterContext viewScope) {
    return ExpressionTreeRewriter.rewriteWith(new TableExpressionRewriter(), expression, viewScope);
  }

  @Override
  public RewriterContext visitTable(Table node, ViewRewriterContext context) {
//    Preconditions.checkState(node.getName().getParts().size() == 1, "Table paths tbd");
    Optional<ViewTable> table = this.plan.getTableByName(node.getName());
    Preconditions.checkState(table.isPresent(), "Could not find table %s", node.getName());
//
//    return new RewriterContext(
//        new Table(NamePath.of(Name.of(table.get().getTableName(), NameCanonicalizer.AS_IS)),
//        table.get(),
//        table.get().getColumns()
//      );
    return null;
  }

  public class TableExpressionRewriter
      extends ExpressionRewriter<RewriterContext> {

    @Override
    public Expression rewriteFunctionCall(FunctionCall node, RewriterContext context,
        ExpressionTreeRewriter<RewriterContext> treeRewriter) {
      return super.rewriteFunctionCall(node, context, treeRewriter);
    }

    @Override
    public Expression rewriteIdentifier(Identifier node, RewriterContext context,
        ExpressionTreeRewriter<RewriterContext> treeRewriter) {

      Optional<DataColumn> column = context.getTable().getColumn(node.getValue());
      if (column.isEmpty()) throw new RuntimeException(String.format("Could not find column %s", node.getValue()));
      return new Identifier(column.get().getPhysicalName()); //todo: new identifier?
    }
  }

  @Value
  public static class RewriterContext {
    Node node;
    ViewTable table; //todo: to analysis object?
    List<DataColumn> columns;
  }

  @Value
  public static class ViewRewriterContext {
    Scope result;
    Optional<StatementAnalysis> analysis;
    Optional<ImportSchema> schema;
    Optional<ExpressionAnalysis> expressionAnalysis;
  }

  @Value
  public static class ViewTable {
    QualifiedName path;

    String tableName;
    List<DataColumn> columns;

    Optional<Query> queryAst;

    public List<DataColumn> getSqmlColumns() {
      Map<String, DataColumn> cols = columns.stream()
          .filter(c->c.getLogicalName() != null)
          .collect(
          Collectors.toUnmodifiableMap(DataColumn::getLogicalName, Function.identity(),
              (v1, v2) -> v2));
      return new ArrayList<>(cols.values());
    }

    public Optional<DataColumn> getColumn(String logicalName) {
      List<DataColumn> selectionSet = columns;
      for (int i = selectionSet.size() - 1; i >= 0; i--) {
        DataColumn column = selectionSet.get(i);
        if (column.getLogicalName() != null && column.getLogicalName().equals(logicalName)) {
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

  public static class ColumnNameGen {
    public int count = 0;
    public String generateName(SingleColumn name) {
      if (name.getAlias().isPresent()) {
        return name.getAlias().get().getValue() + "_" + (++count);
      } else if (name.getExpression() instanceof Identifier) {
        return ((Identifier)name.getExpression()).getValue() + "_" + (++count);
      }

      return "VAR_" + (++count);
    }
    public String generateName(String name) {
      return name + "_" + (++count);
    }
  }
}