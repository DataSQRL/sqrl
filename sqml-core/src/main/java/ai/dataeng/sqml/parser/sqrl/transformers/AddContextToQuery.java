package ai.dataeng.sqml.parser.sqrl.transformers;

import static ai.dataeng.sqml.parser.sqrl.AliasUtil.primaryKeySelect;
import static ai.dataeng.sqml.parser.sqrl.AliasUtil.selectAliasItem;
import static ai.dataeng.sqml.util.SqrlNodeUtil.groupBy;

import ai.dataeng.sqml.parser.Table;
import ai.dataeng.sqml.parser.sqrl.analyzer.aggs.AggregationDetector;
import ai.dataeng.sqml.parser.sqrl.function.FunctionLookup;
import ai.dataeng.sqml.parser.sqrl.node.PrimaryKeySelectItem;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.name.Name;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class AddContextToQuery {
  AggregationDetector aggregationDetector = new AggregationDetector(new FunctionLookup());

  /**
   * Adds context keys to select. If aggregating, create or append to group by.
   */
  public QuerySpecification transform(QuerySpecification spec, Table table) {
    List<SelectItem> additionalColumns = table.getPrimaryKeys().stream()
        .map(column -> primaryKeySelect(
            Name.SELF_IDENTIFIER.toNamePath().concat(column.getId().toNamePath()),
            column.getId().toNamePath(), column))
        .collect(Collectors.toList());
    List<SelectItem> list = new ArrayList<>(spec.getSelect().getSelectItems());
    list.addAll(additionalColumns);

    Select select = new Select(spec.getSelect().getLocation(), spec.getSelect().isDistinct(), list);

    Optional<GroupBy> groupBy = spec.getGroupBy();
    if (aggregationDetector.isAggregating(spec.getSelect())) {
      groupBy = Optional.of(appendGroupBy(spec.getGroupBy(), select));
    }

    QuerySpecification querySpecification = new QuerySpecification(
        spec.getLocation(),
        select,
        spec.getFrom(),
        spec.getWhere(),
        groupBy,
        spec.getHaving(),
        spec.getOrderBy(),
        spec.getLimit()
    );
    return querySpecification;
  }

  /**
   * Add the index of the group columns we just created.
   */
  private GroupBy appendGroupBy(Optional<GroupBy> groupBy, Select select) {
    List<Expression> grouping = new ArrayList<>(
        groupBy
            .map(g->g.getGroupingElement().getExpressions())
            .orElse(List.of()));

    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);
      if (selectItem instanceof PrimaryKeySelectItem) {
        grouping.add(new LongLiteral(Integer.toString(i + 1)));
      }
    }

    return groupBy(grouping);
  }
}
