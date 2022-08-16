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

import static java.util.Objects.requireNonNull;

import ai.datasqrl.parse.tree.name.Name;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;

@Getter
public class SingleColumn
    extends SelectItem {

  private final Optional<Identifier> alias;
  private final String expression;
  SqlNode sqlNode = null;

  public SingleColumn(String expression) {
    this(Optional.empty(), expression, Optional.empty());
  }

  public SingleColumn(String expression, Optional<Identifier> alias) {
    this(Optional.empty(), expression, alias);
  }

  public SingleColumn(NodeLocation location, String expression, Optional<Identifier> alias) {
    this(Optional.of(location), expression, alias);
  }

  public SingleColumn(Optional<NodeLocation> location, String expression,
      Optional<Identifier> alias) {
    super(location);
    requireNonNull(expression, "expression is null");
    requireNonNull(alias, "alias is null");

    this.expression = expression;
    this.alias = alias;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SingleColumn other = (SingleColumn) obj;
    return Objects.equals(this.alias, other.alias) && Objects
        .equals(this.expression, other.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hash(alias, expression);
  }

  @Override
  public String toString() {
    if (alias.isPresent()) {
      return expression.toString() + " " + alias.get();
    }

    return expression.toString();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitSingleColumn(this, context);
  }
}
