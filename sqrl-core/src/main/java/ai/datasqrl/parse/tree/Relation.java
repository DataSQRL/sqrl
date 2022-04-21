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

import java.util.Optional;

public abstract class Relation
    extends Node {

  private Object resolved;

  protected Relation(Optional<NodeLocation> location) {
    super(location);
  }

  protected Relation(Optional<NodeLocation> location, Object resolved) {
    super(location);
    this.resolved = resolved;
  }

  public <T> T getResolved() {
    return (T) resolved;
  }

  public void setResolved(Object resolved) {
    this.resolved = resolved;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitRelation(this, context);
  }
}
