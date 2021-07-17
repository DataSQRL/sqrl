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
package ai.dataeng.sqml.planner.optimizations;

import static ai.dataeng.sqml.plan.ChildReplacer.replaceChildren;
import static ai.dataeng.sqml.planner.iterative.Lookup.noLookup;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class PlanNodeSearcher {

  private final PlanNode node;
  private final Lookup lookup;
  private Predicate<PlanNode> where = alwaysTrue();
  private Predicate<PlanNode> recurseOnlyWhen = alwaysTrue();
  private PlanNodeSearcher(PlanNode node, Lookup lookup) {
    this.node = requireNonNull(node, "node is null");
    this.lookup = requireNonNull(lookup, "lookup is null");
  }

  public static PlanNodeSearcher searchFrom(PlanNode node) {
    return searchFrom(node, noLookup());
  }

  /**
   * Use it in optimizer {@link com.facebook.presto.sql.planner.iterative.Rule} only if you truly do
   * not have a better option
   * <p>
   * TODO: replace it with a support for plan (physical) properties in rules pattern matching
   */
  public static PlanNodeSearcher searchFrom(PlanNode node, Lookup lookup) {
    return new PlanNodeSearcher(node, lookup);
  }

  public PlanNodeSearcher where(Predicate<PlanNode> where) {
    this.where = requireNonNull(where, "where is null");
    return this;
  }

  public PlanNodeSearcher recurseOnlyWhen(Predicate<PlanNode> skipOnly) {
    this.recurseOnlyWhen = requireNonNull(skipOnly, "recurseOnlyWhen is null");
    return this;
  }

  public <T extends PlanNode> Optional<T> findFirst() {
    return findFirstRecursive(node);
  }

  private <T extends PlanNode> Optional<T> findFirstRecursive(PlanNode node) {
    node = lookup.resolve(node);

    if (where.test(node)) {
      return Optional.of((T) node);
    }
    if (recurseOnlyWhen.test(node)) {
      for (PlanNode source : node.getSources()) {
        Optional<T> found = findFirstRecursive(source);
        if (found.isPresent()) {
          return found;
        }
      }
    }
    return Optional.empty();
  }

}
