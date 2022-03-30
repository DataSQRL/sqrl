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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class DereferenceExpression
    extends Expression {

  private final Expression base;
  private final Identifier field;

  public DereferenceExpression(Expression base, Identifier field) {
    this(Optional.empty(), base, field);
  }

  public DereferenceExpression(NodeLocation location, Expression base, Identifier field) {
    this(Optional.of(location), base, field);
  }

  private DereferenceExpression(Optional<NodeLocation> location, Expression base,
      Identifier field) {
    super(location);
    checkArgument(base != null, "base is null");
    checkArgument(field != null, "fieldName is null");
    this.base = base;
    this.field = field;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDereferenceExpression(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(base);
  }

  public Expression getBase() {
    return base;
  }

  public Identifier getField() {
    return field;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DereferenceExpression that = (DereferenceExpression) o;
    return Objects.equals(base, that.base) &&
        Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(base, field);
  }
}
