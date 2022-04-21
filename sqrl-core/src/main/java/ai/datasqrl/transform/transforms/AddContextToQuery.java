package ai.datasqrl.transform.transforms;

import static ai.datasqrl.parse.util.SqrlNodeUtil.groupBy;

import ai.datasqrl.function.FunctionLookup;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import ai.datasqrl.util.AliasGenerator;
import ai.datasqrl.validate.aggs.AggregationDetector;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AddContextToQuery {

  AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());
  AliasGenerator gen = new AliasGenerator();

  /**
   * Adds context keys to select. If aggregating, create or append to group by.
   * <p>
   * Since we are adding columns, we need to assure that they do not collide with existing fields.
   */
  public QuerySpecification transform(QuerySpecification spec, Table table) {
    return transform(spec, table, Name.SELF_IDENTIFIER);
  }

  public QuerySpecification transform(QuerySpecification spec, Table table, Name firstAlias) {
    List<SelectItem> additionalColumns = table.getPrimaryKeys().stream()
        .map(column -> primaryKeySelect(
            firstAlias.toNamePath().concat(column.getId().toNamePath()),
            gen.nextAliasName().toNamePath(), column))
        .collect(Collectors.toList());
    List<SelectItem> list = new ArrayList<>(spec.getSelect().getSelectItems());
    list.addAll(additionalColumns);

    Select select = new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(), list);

    Optional<GroupBy> groupBy = spec.getGroupBy();
    Optional<OrderBy> orderBy = spec.getOrderBy();
    if (aggregationDetector.isAggregating(spec.getSelect())) {
      groupBy = Optional.of(appendGroupBy(spec.getGroupBy(), select));
      orderBy = appendOrderBy(spec.getOrderBy(), select);
    }

    QuerySpecification querySpecification = new QuerySpecification(
        spec.getLocation(),
        select,
        spec.getFrom(),
        spec.getWhere(),
        groupBy,
        spec.getHaving(),
        orderBy,
        spec.getLimit()
    );
    return querySpecification;
  }

  private Optional<OrderBy> appendOrderBy(Optional<OrderBy> orderBy, Select select) {
    if (orderBy.isEmpty()) {
      return orderBy;
    }
    List<SortItem> sortItems = new ArrayList<>();

    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);
      SingleColumn singleColumn = (SingleColumn) selectItem;
      if (isParentPrimaryKey(singleColumn)) {
        sortItems.add(new SortItem(Optional.empty(), new LongLiteral(Integer.toString(i + 1)),
            Optional.empty()));
      }
    }

    if (orderBy.isPresent()) {
      sortItems.addAll(orderBy.get().getSortItems());
    }

    return Optional.of(new OrderBy(Optional.empty(), sortItems));
  }

  private boolean isParentPrimaryKey(SingleColumn singleColumn) {
    return singleColumn.getExpression() instanceof Identifier &&
        ((Identifier) singleColumn.getExpression()).getResolved() instanceof Column &&
        ((Column) ((Identifier) singleColumn.getExpression()).getResolved()).isParentPrimaryKey();
  }

  /**
   * Add the index of the group columns we just created.
   */
  private GroupBy appendGroupBy(Optional<GroupBy> groupBy, Select select) {
    List<Expression> grouping = new ArrayList<>(
        groupBy
            .map(g -> g.getGroupingElement().getExpressions())
            .orElse(new ArrayList<>()));

    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);
      SingleColumn singleColumn = (SingleColumn) selectItem;
      if (isParentPrimaryKey(singleColumn)) {
        grouping.add(new LongLiteral(Integer.toString(i + 1)));
      }
    }

    return groupBy(grouping);
  }

  public static SelectItem primaryKeySelect(NamePath name, NamePath alias, Column column) {
    Identifier identifier = new Identifier(Optional.empty(), name);
    Identifier aliasIdentifier = new Identifier(Optional.empty(), alias);
    Column ppk = column.copy();
    ppk.setParentPrimaryKey(true);
    ppk.setSource(column);
    identifier.setResolved(ppk);
    aliasIdentifier.setResolved(ppk);
    return new SingleColumn(identifier, aliasIdentifier);
  }
}
