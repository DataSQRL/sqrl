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

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class AllColumns
    extends SelectItem {

  private final Optional<NamePath> prefix;

  public AllColumns() {
    super(Optional.empty());
    prefix = Optional.empty();
  }

  public AllColumns(Optional<NamePath> prefix) {
    super(Optional.empty());
    this.prefix = prefix;
  }

  public AllColumns(NodeLocation location) {
    super(Optional.of(location));
    prefix = Optional.empty();
  }

  public AllColumns(NamePath prefix) {
    this(Optional.empty(), prefix);
  }

  public AllColumns(NodeLocation location, NamePath prefix) {
    this(Optional.of(location), prefix);
  }

  private AllColumns(Optional<NodeLocation> location, NamePath prefix) {
    super(location);
    requireNonNull(prefix, "prefix is null");
    this.prefix = Optional.of(prefix);
  }

  public Optional<NamePath> getPrefix() {
    return prefix;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitAllColumns(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AllColumns that = (AllColumns) o;
    return Objects.equals(prefix, that.prefix);
  }

  @Override
  public int hashCode() {
    return prefix.hashCode();
  }

  @Override
  public String toString() {
    if (prefix.isPresent()) {
      return prefix.get() + ".*";
    }

    return "*";
  }
}
