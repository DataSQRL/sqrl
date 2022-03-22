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

import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InlineJoinBody
    extends Expression {

  private final NamePath table;
  private final Optional<Identifier> alias;
  private final Expression criteria;
  private final Optional<InlineJoinBody> inlineJoinBody;

  public InlineJoinBody(Optional<NodeLocation> location, NamePath table,
      Optional<Identifier> alias,Expression criteria, Optional<InlineJoinBody> inlineJoinBody) {
    super(location);
    this.table = table;
    this.alias = alias;
    this.criteria = criteria;
    this.inlineJoinBody = inlineJoinBody;
  }

  public NamePath getTable() {
    return table;
  }

  public Optional<Identifier> getAlias() {
    return alias;
  }

  public Expression getCriteria() {
    return criteria;
  }

  public Optional<InlineJoinBody> getInlineJoinBody() {
    return inlineJoinBody;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitInlineJoinBody(this, context);
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
    InlineJoinBody that = (InlineJoinBody) o;
    return Objects.equals(table, that.table) && Objects.equals(alias, that.alias)
        && Objects.equals(criteria, that.criteria)
        ;
  }

  @Override
  public int hashCode() {
    return Objects.hash(table, alias, criteria);
  }
}
