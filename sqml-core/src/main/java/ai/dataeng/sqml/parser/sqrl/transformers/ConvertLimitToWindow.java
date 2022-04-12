package ai.dataeng.sqml.parser.sqrl.transformers;

import static ai.dataeng.sqml.util.SqrlNodeUtil.and;
import static ai.dataeng.sqml.util.SqrlNodeUtil.ident;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.ComparisonExpression.Operator;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Limit;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.Window;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
    List<SelectItem> items = alias(spec.getSelect().getSelectItems());
    items.add(new SingleColumn(new FunctionCall(Optional.empty(),
        NamePath.of("ROW_NUMBER"), List.of(), false, Optional.of(new Window(
          getSelectExpressions(spec.getSelect().getSelectItems(), spec.getParentPrimaryKeys()),
          Optional.of(getOrder(spec.getOrderBy()))))),
        new Identifier(Optional.empty(), rowNum.toNamePath())
        ));

    QuerySpecification inner = new QuerySpecification(
        spec,
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
        spec,
        new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(),
            project(spec.getSelect().getSelectItems(), items)),
        subquery,
        Optional.of(and(limitToRowNumCondition(spec.getLimit()), spec.getHaving())),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    QuerySpecification outer2 = new QuerySpecification(
        spec,
        spec.getSelect(),
        new TableSubquery(new Query(outer, Optional.empty(), Optional.empty())),
        spec.getWhere(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty()
    );

    return outer2;
  }

  private OrderBy getOrder(Optional<OrderBy> orderBy) {
    if (orderBy.isPresent()) return orderBy.get();
    return new OrderBy(List.of(new SortItem(ident(Name.SELF_IDENTIFIER.toNamePath().concat(Name.INGEST_TIME)), Ordering.DESCENDING)));
  }

  private List<SelectItem> project(List<SelectItem> selectItems, List<SelectItem> aliases) {
    int pkNum = 0;
    List<SelectItem> newSelect = new ArrayList<>();
    for (int i = 0; i < selectItems.size(); i++) {
      SingleColumn col = (SingleColumn) selectItems.get(i);
      SingleColumn alias = (SingleColumn) aliases.get(i);
      if (alias.getExpression() instanceof Identifier &&
          ((Identifier) alias.getExpression()).getNamePath().getFirst().equals(Name.SELF_IDENTIFIER)) {
        newSelect.add(new SingleColumn(col.getLocation(),
            new Identifier(Optional.empty(), alias.getAlias().get().getNamePath()),
            Optional.of(ident(Name.system("_pk"+pkNum++)))));
      } else {
        newSelect.add(new SingleColumn(col.getLocation(),
            new Identifier(Optional.empty(), alias.getAlias().get().getNamePath()),
            Optional.of(getColumnName(col))));
      }
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

  private List<SelectItem> alias(List<SelectItem> selectItems) {
    return selectItems.stream()
        .map(s->new SingleColumn(s.getLocation(), ((SingleColumn)s).getExpression(),
            Optional.of(ident(gen.nextAliasName()))))
        .collect(Collectors.toList());
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
