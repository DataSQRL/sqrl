package ai.dataeng.sqml.parser.sqrl.transformers;

import static ai.dataeng.sqml.parser.sqrl.AliasUtil.primaryKeySelect;
import static ai.dataeng.sqml.util.SqrlNodeUtil.groupBy;

import ai.dataeng.sqml.parser.AliasGenerator;
import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.aggs.AggregationDetector;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.node.PrimaryKeySelectItem;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AddContextToQuery {
  AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());
  AliasGenerator gen = new AliasGenerator();
  /**
   * Adds context keys to select. If aggregating, create or append to group by.
   *
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
    List<Integer> parentPrimaryKeys = IntStream.range(spec.getSelect().getSelectItems().size(),
        list.size()).boxed().collect(Collectors.toList());
    spec.setParentPrimaryKeys(parentPrimaryKeys);
    System.out.println(additionalColumns);
    Select select = new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(), list);

    Optional<GroupBy> groupBy = spec.getGroupBy();
    Optional<OrderBy> orderBy = spec.getOrderBy();
    if (aggregationDetector.isAggregating(spec.getSelect())) {
      groupBy = Optional.of(appendGroupBy(spec.getGroupBy(), select));
      orderBy = appendOrderBy(spec.getOrderBy(), select);
    }

    QuerySpecification querySpecification = new QuerySpecification(
        spec,
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
    if (orderBy.isEmpty()) return orderBy;
    List<SortItem> sortItems = new ArrayList<>();

    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);
      if (selectItem instanceof PrimaryKeySelectItem) {
        sortItems.add(new SortItem(Optional.empty(), new LongLiteral(Integer.toString(i + 1)), Optional.empty()));
      }
    }

    if (orderBy.isPresent()) {
      sortItems.addAll(orderBy.get().getSortItems());
    }

    return Optional.of(new OrderBy(Optional.empty(), sortItems));
  }

  /**
   * Add the index of the group columns we just created.
   */
  private GroupBy appendGroupBy(Optional<GroupBy> groupBy, Select select) {
    List<Expression> grouping = new ArrayList<>(
        groupBy
            .map(g->g.getGroupingElement().getExpressions())
            .orElse(new ArrayList<>()));

    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);
      if (selectItem instanceof PrimaryKeySelectItem) {
        grouping.add(new LongLiteral(Integer.toString(i + 1)));
      }
    }

    return groupBy(grouping);
  }
}
