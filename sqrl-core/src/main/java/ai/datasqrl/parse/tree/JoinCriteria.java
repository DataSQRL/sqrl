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

import java.util.List;
import java.util.Optional;

public abstract class JoinCriteria extends Node {
  public static JoinOn JOIN_ON_TRUE = new JoinOn(Optional.empty(), BooleanLiteral.TRUE_LITERAL);

  protected JoinCriteria(Optional<NodeLocation> location) {
    super(location);
  }

  // Force subclasses to have a proper equals and hashcode implementation
  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract int hashCode();

  public abstract List<Node> getNodes();
}
