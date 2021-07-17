/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.dataeng.sqml.planner;

import static ai.dataeng.sqml.relational.Expressions.variable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.forEachPair;

import ai.dataeng.sqml.common.SortOrder;
import ai.dataeng.sqml.plan.Ordering;
import ai.dataeng.sqml.plan.OrderingScheme;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.OrderBy;
import ai.dataeng.sqml.sql.tree.SortItem;
import ai.dataeng.sqml.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PlannerUtils {

  private PlannerUtils() {
  }

  public static SortOrder toSortOrder(SortItem sortItem) {
    if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
      return SortOrder.ASC_NULLS_LAST;
    }
    return SortOrder.DESC_NULLS_LAST;
  }

  public static OrderingScheme toOrderingScheme(OrderBy orderBy, TypeProvider types) {
    return toOrderingScheme(
        orderBy.getSortItems().stream()
            .map(SortItem::getSortKey)
            .map(item -> {
              checkArgument(item instanceof SymbolReference, "must be symbol reference");
              return variable(((SymbolReference) item).getName(), types.get(item));
            }).collect(toImmutableList()),
        orderBy.getSortItems().stream()
            .map(PlannerUtils::toSortOrder)
            .collect(toImmutableList()));
  }

  public static OrderingScheme toOrderingScheme(List<VariableReferenceExpression> orderingSymbols,
      List<SortOrder> sortOrders) {
    ImmutableList.Builder<Ordering> builder = ImmutableList.builder();

    // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
    Set<VariableReferenceExpression> keysSeen = new HashSet<>();

    forEachPair(orderingSymbols.stream(), sortOrders.stream(), (variable, sortOrder) -> {
      if (!keysSeen.contains(variable)) {
        keysSeen.add(variable);
        builder.add(new Ordering(variable, sortOrder));
      }
    });

    return new OrderingScheme(builder.build());
  }
}
