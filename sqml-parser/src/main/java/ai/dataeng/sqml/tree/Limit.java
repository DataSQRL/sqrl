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

import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

import io.airlift.slice.Slice;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Limit
    extends Node {

  private final String value;

  public Limit(String value) {
    this(Optional.empty(), value);
  }

  public Limit(NodeLocation location, String value) {
    this(Optional.of(location), value);
  }

  private Limit(Optional<NodeLocation> location, String value) {
    super(location);
    requireNonNull(value, "value is null");
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLimitNode(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Limit that = (Limit) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
