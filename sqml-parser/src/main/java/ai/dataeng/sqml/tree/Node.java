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

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Optional;

public abstract class Node {

  private final Optional<NodeLocation> location;

  protected Node(Optional<NodeLocation> location) {
    this.location = requireNonNull(location, "location is null");
  }

  /**
   * Accessible for {@link AstVisitor}, use {@link AstVisitor#process(Node, Object)} instead.
   */
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

  public Optional<NodeLocation> getLocation() {
    return location;
  }

  public abstract List<? extends Node> getChildren();

  // Force subclasses to have a proper equals and hashcode implementation
  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public String toString() {
    return NodeFormatter.accept(this);
  }

  public List<Annotation> getAnnotations() {
    return List.of();
  }
}
