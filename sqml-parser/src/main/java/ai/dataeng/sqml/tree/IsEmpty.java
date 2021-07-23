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

public class IsEmpty
    extends Expression {

  private final boolean notEmpty;

  public IsEmpty(Optional<NodeLocation> location, boolean notEmpty) {
    super(location);
    this.notEmpty = notEmpty;
  }

  public boolean isNotEmpty() {
    return notEmpty;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitIsEmpty(this, context);
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
    IsEmpty isEmpty = (IsEmpty) o;
    return notEmpty == isEmpty.notEmpty;
  }

  @Override
  public int hashCode() {
    return Objects.hash(notEmpty);
  }
}
