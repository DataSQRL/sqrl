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

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Identifier
    extends Expression {

  private final NamePath namePath;

  public Identifier(Name name) {
    super(Optional.empty());
    this.namePath = name.toNamePath();
  }

  public Identifier(NodeLocation location, NamePath namePath) {
    super(Optional.of(location));
    this.namePath = namePath;
  }

  public Identifier(Optional<NodeLocation> location, NamePath namePath) {
    super(location);
    this.namePath = namePath;
  }
  public Identifier(Optional<NodeLocation> location, Name name) {
    super(location);
    this.namePath = name.toNamePath();
  }

  public NamePath getNamePath() {
    return namePath;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIdentifier(this, context);
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
    Identifier that = (Identifier) o;
    return Objects.equals(namePath, that.namePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namePath);
  }

  @Override
  public String toString() {
    return namePath.toString();
  }
}
