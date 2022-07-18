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

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TableNode
    extends QueryBody {

  private final NamePath name;
  private final Optional<Name> alias;
  private final List<Hint> hints;

  public TableNode(NodeLocation location, NamePath name,
      Optional<Name> alias, List<Hint> hints) {
    this(Optional.of(location), name, alias, hints);
  }

  public TableNode(Optional<NodeLocation> location, NamePath name, Optional<Name> alias) {
    this(location, name, alias, List.of());
  }

  public TableNode(Optional<NodeLocation> location, NamePath name, Optional<Name> alias, List<Hint> hints) {
    super(location);
    this.name = name;
    this.alias = alias;
    this.hints = hints;
  }

  public NamePath getNamePath() {
    return name;
  }

  public Optional<Name> getAlias() {
    return alias;
  }

  public List<Hint> getHints() {
    return hints;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitTableNode(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .addValue(name)
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

    return this == o;
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
