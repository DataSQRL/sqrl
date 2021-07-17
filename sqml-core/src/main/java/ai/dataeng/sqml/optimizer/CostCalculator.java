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

package ai.dataeng.sqml.optimizer;


import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.plan.PlanNode;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface CostCalculator
{
    /**
     * Calculates cumulative cost of a node.
     * @param node The node to compute cost for.
     * @param stats The stats provider for node's stats and child nodes' stats, to be used if stats are needed to compute cost for the {@code node}
     */
    PlanCostEstimate calculateCost(
            PlanNode node,
            StatsProvider stats,
            CostProvider sourcesCosts,
            Session session);

    @Target(PARAMETER)
    @Retention(RUNTIME)
    @interface EstimatedExchanges {}
}
