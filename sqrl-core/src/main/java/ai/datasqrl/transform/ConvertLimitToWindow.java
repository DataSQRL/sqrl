package ai.datasqrl.transform;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;
import static ai.datasqrl.parse.util.SqrlNodeUtil.ident;

import ai.datasqrl.util.AliasGenerator;
import ai.datasqrl.schema.Table;
import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.SortItem.Ordering;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.Window;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class ConvertLimitToWindow {
  AliasGenerator gen = new AliasGenerator();
  Name rowNum = Name.system("_row_num");
  /**
   * https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/queries/topn/
   *
   * SELECT product_id, category, product_name, sales
   * FROM (
   *   SELECT *,
   *     ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num
   *   FROM ShopSales)
   * WHERE row_num <= 5
   */
  public QuerySpecification transform(QuerySpecification spec, Table table) {
    List<SelectItem> items = new ArrayList<>(spec.getSelect().getSelectItems());//randomAliasSelectList(spec.getSelect().getSelectItems());
    items.add(new SingleColumn(new FunctionCall(Optional.empty(),
        NamePath.of("ROW_NUMBER"), List.of(), false, Optional.of(new Window(
          getSelectExpressions(spec.getSelect().getSelectItems(), table.getParentPrimaryKeys()),
          Optional.of(getOrder(spec.getOrderBy()))))),
        new Identifier(Optional.empty(), rowNum.toNamePath())));

    QuerySpecification inner = new QuerySpecification(
        Optional.empty(),
        new Select(items),
        spec.getFrom(),
        spec.getWhere(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());

    Name tableAlias = gen.nextTableAliasName();
    Relation subquery = new AliasedRelation(new TableSubquery(new Query(inner, Optional.empty(), Optional.empty())), ident(tableAlias));

    QuerySpecification outer = new QuerySpecification(
        Optional.empty(),
        new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(),
            project(spec.getSelect().getSelectItems(), items)),
        subquery,
        Optional.of(and(limitToRowNumCondition(spec.getLimit()), spec.getHaving())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    return outer;
  }

  private OrderBy getOrder(Optional<OrderBy> orderBy) {
    if (orderBy.isPresent()) return orderBy.get();
    return new OrderBy(List.of(new SortItem(ident(Name.SELF_IDENTIFIER.toNamePath().concat(Name.INGEST_TIME)), Ordering.DESCENDING)));
  }

  private List<SelectItem> project(List<SelectItem> selectItems, List<SelectItem> aliases) {
    List<SelectItem> newSelect = new ArrayList<>();
    for (int i = 0; i < selectItems.size(); i++) {
      SingleColumn col = (SingleColumn) selectItems.get(i);
      SingleColumn a = ((SingleColumn)aliases.get(i));
      Identifier ident = new Identifier(Optional.empty(), getColumnName(col).getNamePath());
      ident.setResolved(((Identifier)a.getExpression()).getResolved());
      newSelect.add(new SingleColumn(col.getLocation(),
          ident,
          Optional.empty()));
    }
    return newSelect;
  }

  private Identifier getColumnName(SingleColumn col) {
    if (col.getAlias().isPresent()) {
      return col.getAlias().get();
    }
    if (col.getExpression() instanceof Identifier) {
      return new Identifier(Optional.empty(),
          ((Identifier) col.getExpression()).getNamePath().getLast().toNamePath());
    }
    throw new RuntimeException("Could not resolve identifier name");
  }

  private Expression limitToRowNumCondition(Optional<Limit> limit) {
    return new ComparisonExpression(Operator.LESS_THAN_OR_EQUAL, new Identifier(Optional.empty(),
        rowNum.toNamePath()), new LongLiteral(limit.get().getValue()));
  }

  private List<Expression> getSelectExpressions(
      List<SelectItem> selectItems, List<Integer> parentPrimaryKeys) {
    List<Expression> expressions = new ArrayList<>();
    for (Integer ordinal : parentPrimaryKeys) {
      expressions.add(((SingleColumn)selectItems.get(ordinal)).getExpression());
    }
    return expressions;
  }
}
