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
package ai.dataeng.sqml.planner.optimizations;

import static ai.dataeng.sqml.plan.JoinNode.Type.FULL;
import static ai.dataeng.sqml.plan.JoinNode.Type.INNER;
import static ai.dataeng.sqml.plan.JoinNode.Type.LEFT;
import static ai.dataeng.sqml.plan.JoinNode.Type.RIGHT;

import ai.dataeng.sqml.plan.JoinNode;
import ai.dataeng.sqml.sql.tree.Join;

public final class JoinNodeUtils {

  private JoinNodeUtils() {
  }

  public static JoinNode.Type typeConvert(Join.Type joinType) {
    // Omit SEMI join types because they must be inferred by the planner and not part of the SQL parse tree
    switch (joinType) {
      case CROSS:
      case IMPLICIT:
      case INNER:
        return INNER;
      case LEFT:
        return LEFT;
      case RIGHT:
        return RIGHT;
      case FULL:
        return FULL;
      default:
        throw new UnsupportedOperationException("Unsupported join type: " + joinType);
    }
  }
}
