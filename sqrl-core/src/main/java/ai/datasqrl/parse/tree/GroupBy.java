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
import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GroupBy
    extends Node {

  private final GroupingElement groupingElement;

  public GroupBy(GroupingElement groupingElements) {
    this(Optional.empty(), groupingElements);
  }

  public GroupBy(NodeLocation location,
      GroupingElement groupingElement) {
    this(Optional.of(location), groupingElement);
  }

  public GroupBy(Optional<NodeLocation> location,
      GroupingElement groupingElement) {
    super(location);
    this.groupingElement = requireNonNull(groupingElement);
  }

  public GroupingElement getGroupingElement() {
    return groupingElement;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitGroupBy(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of(groupingElement);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GroupBy groupBy = (GroupBy) o;
    return Objects.equals(groupingElement, groupBy.groupingElement);
  }

  @Override
  public int hashCode() {
    return Objects.hash(groupingElement);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("groupingElement", groupingElement)
        .toString();
  }
}
