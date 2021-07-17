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
package ai.dataeng.sqml.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class NestedSelect
    extends SelectItem {

  private final Optional<Identifier> alias;
  private final Node rel;
  private final List<SelectItem> columns;

  public NestedSelect(Optional<NodeLocation> location,
      Optional<Identifier> alias, Node rel,
      List<SelectItem> columns) {
    super(location);
    this.alias = alias;
    this.rel = rel;
    this.columns = columns;
  }

  public Optional<Identifier> getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    NestedSelect that = (NestedSelect) o;
    return Objects.equals(alias, that.alias) && Objects.equals(rel, that.rel)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, rel, columns);
  }

  @Override
  public String toString() {
    return "NestedSelect{" +
        "alias=" + alias +
        ", rel=" + rel +
        ", columns=" + columns +
        '}';
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNestedSelect(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }
}
