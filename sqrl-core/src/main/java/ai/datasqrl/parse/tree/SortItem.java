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
package ai.datasqrl.parse.tree;

import static com.google.common.base.MoreObjects.toStringHelper;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class SortItem
    extends Node {

  private final Expression sortKey;
  private final Optional<Ordering> ordering;

  public SortItem(Expression sortKey, Ordering ordering) {
    this(Optional.empty(), sortKey, ordering);
  }

  public SortItem(NodeLocation location, Expression sortKey, Ordering ordering) {
    this(Optional.of(location), sortKey, ordering);
  }

  public SortItem(Optional<NodeLocation> location, Expression sortKey, Ordering ordering) {
    super(location);
    this.ordering = Optional.of(ordering);
    this.sortKey = sortKey;
  }

  public SortItem(Optional<NodeLocation> location, Expression sortKey,
      Optional<Ordering> ordering) {
    super(location);
    this.ordering = ordering;
    this.sortKey = sortKey;
  }

  //todo: migrate to identifier
  public Expression getSortKey() {
    return sortKey;
  }

  public Optional<Ordering> getOrdering() {
    return ordering;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSortItem(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(sortKey);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("sortKey", sortKey)
        .add("ordering", ordering)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SortItem sortItem = (SortItem) o;
    return Objects.equals(sortKey, sortItem.sortKey) &&
        (ordering == sortItem.ordering);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sortKey, ordering);
  }

  public enum Ordering {
    ASCENDING, DESCENDING
  }
}
