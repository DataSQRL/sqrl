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

import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class FunctionCall
    extends Expression {

  private final NamePath name;
  private final List<Expression> arguments;
  private final boolean distinct;
  private final Optional<Window> over;

  public FunctionCall(NamePath name, List<Expression> arguments,
      boolean distinct) {
    this(Optional.empty(), name, arguments, distinct, Optional.empty());
  }
  public FunctionCall(NodeLocation location, NamePath name, List<Expression> arguments,
      boolean distinct) {
    this(Optional.of(location), name, arguments, distinct, Optional.empty());
  }

  public FunctionCall(Optional<NodeLocation> location, NamePath name,
      List<Expression> arguments, boolean distinct, Optional<Window> over) {
    super(location);
    this.distinct = distinct;
    this.over = over;
    requireNonNull(name, "name is null");
    requireNonNull(arguments, "arguments is null");

    this.name = name;
    this.arguments = arguments;
  }

  public NamePath getNamePath() {
    return name;
  }

  public List<Expression> getArguments() {
    return arguments;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public Optional<Window> getOver() {
    return over;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitFunctionCall(this, context);
  }

  @Override
  public List<Node> getChildren() {
    ImmutableList.Builder<Node> nodes = ImmutableList.builder();
    nodes.addAll(arguments);
    return nodes.build();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    FunctionCall o = (FunctionCall) obj;
    return Objects.equals(name, o.name) &&
        Objects.equals(arguments, o.arguments);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, arguments);
  }
}
