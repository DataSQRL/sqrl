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

public class InlineJoin
    extends Relation {

  private final QualifiedName table;
  private final Optional<Identifier> alias;
  private final Expression on;
  private final Optional<QualifiedName> inverse;
  private final Optional<Integer> limit;

  public InlineJoin(Optional<NodeLocation> location, QualifiedName table,
      Optional<Identifier> alias, Expression on, Optional<QualifiedName> inverse,
      Optional<Integer> limit) {
    super(location);
    this.table = table;
    this.alias = alias;
    this.on = on;
    this.inverse = inverse;
    this.limit = limit;
  }

  public QualifiedName getTable() {
    return table;
  }

  public Optional<Identifier> getAlias() {
    return alias;
  }

  public Expression getCriteria() {
    return on;
  }

  //todo inverse not a qualified name
  public Optional<QualifiedName> getInverse() {
    return inverse;
  }

  public Optional<Integer> getLimit() {
    return limit;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInlineJoin(this, context);
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
    InlineJoin that = (InlineJoin) o;
    return Objects.equals(table, that.table) && Objects.equals(alias, that.alias)
        && Objects.equals(on, that.on) && Objects.equals(inverse, that.inverse)
        && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, alias, on, inverse, limit);
  }
}
