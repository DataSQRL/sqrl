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
package ai.dataeng.sqml.plan;

import ai.dataeng.sqml.planner.UnnestNode;
import ai.dataeng.sqml.planner.iterative.GroupReference;

public abstract class InternalPlanVisitor<R, C>
    extends PlanVisitor<R, C>
{
  public R visitOutput(OutputNode node, C context)
  {
    return visitPlan(node, context);
  }

  public R visitJoin(JoinNode node, C context)
  {
    return visitPlan(node, context);
  }

  public R visitSort(SortNode node, C context)
  {
    return visitPlan(node, context);
  }

  public R visitUnnest(UnnestNode node, C context)
  {
    return visitPlan(node, context);
  }

  public R visitGroupId(GroupIdNode node, C context)
  {
    return visitPlan(node, context);
  }

  public R visitEnforceSingleRow(EnforceSingleRowNode node, C context)
  {
    return visitPlan(node, context);
  }
  public R visitApply(ApplyNode node, C context)
  {
    return visitPlan(node, context);
  }
  public R visitGroupReference(GroupReference node, C context)
  {
    return visitPlan(node, context);
  }
  public R visitLateralJoin(LateralJoinNode node, C context)
  {
    return visitPlan(node, context);
  }

}
