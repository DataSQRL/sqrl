package ai.datasqrl.plan.local.transpiler;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.ComparisonExpression.Operator;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SelectItem;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class NormalizerUtils {

  /**
   * Grouping columns can be either by expression or by alias name. Ordinals start at 1.
   */
  public static Set<Integer> mapGroupByToOrdinal(
      Optional<GroupBy> groupBy, Select select, List<SingleColumn> selectList) {
    if (groupBy.isEmpty()) {
      return Set.of();
    }
    Set<Integer> ordinals = new HashSet<>();
    List<Expression> groupingExpressions = groupBy.get().getGroupingElement().getExpressions();

    for (Expression groupingExpression : groupingExpressions) {
      if (groupingExpression instanceof LongLiteral) {
        throw new RuntimeException("Ordinals in grouping expressions");
      }

      //Grouping expression is a column or alias reference
      if (groupingExpression instanceof Identifier) {
        Identifier groupIdentifier = (Identifier) groupingExpression;
        // give preference to aliased columns
        Optional<Integer> aliasedColumnIndex = indexOfAliasedColumn(select, groupIdentifier);
        if (aliasedColumnIndex.isPresent()) {
          ordinals.add(aliasedColumnIndex.get());
          continue;
        }
      }

      // otherwise, find item in select list
      int index = selectList.stream()
          .map(SingleColumn::getExpression)
          .collect(Collectors.toList())
          .indexOf(groupingExpression);
      if (index == -1) {
        throw new RuntimeException("Could not find grouping column");
      }
      ordinals.add(index);
    }

    return ordinals;
  }

  public static Optional<Integer> indexOfAliasedColumn(Select select, Identifier identifier) {
    for (int i = 0; i < select.getSelectItems().size(); i++) {
      SelectItem selectItem = select.getSelectItems().get(i);

      if (selectItem instanceof SingleColumn) {
        SingleColumn singleColumn = (SingleColumn) selectItem;
        if (singleColumn.getAlias().isPresent() && singleColumn.getAlias().get()
            .equals(identifier)) {
          return Optional.of(i);
        }
      } else if (selectItem instanceof AllColumns) {
        //do nothing
      } else {
        throw new RuntimeException("Unknown select item");
      }
    }
    return Optional.empty();
  }

  public static Expression primaryKeyCriteria(Table table, QuerySpecNorm base, TableNodeNorm tableNodeNorm) {
    List<Expression> criteria = new ArrayList<>();
    for (int i = 0; i < table.getPrimaryKeys().size(); i++) {
      Column column = table.getPrimaryKeys().get(i);

      criteria.add(new ComparisonExpression(Operator.EQUAL,
          ResolvedColumn.of(tableNodeNorm, column),
          new ReferenceExpression(base,
              base.getParentPrimaryKeys().get(i))));
    }
    return and(criteria);
  }
}
