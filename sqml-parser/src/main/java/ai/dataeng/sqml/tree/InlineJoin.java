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

import ai.dataeng.sqml.tree.name.Name;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InlineJoin
    extends Declaration {

  private final InlineJoinBody join;
  private final Optional<Name> inverse;

  public InlineJoin(Optional<NodeLocation> location,
      InlineJoinBody join,
      Optional<Name> inverse) {
    super(location);

    this.join = join;
    this.inverse = inverse;
  }

  public InlineJoinBody getJoin() {
    return join;
  }

  public Optional<Name> getInverse() {
    return inverse;
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
    return Objects.equals(join, that.join) && Objects
        .equals(inverse, that.inverse);
  }

  @Override
  public int hashCode() {
    return Objects.hash(join, inverse);
  }
}
