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

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.planner.PlanNodeId;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.List;
import javax.annotation.concurrent.Immutable;

@Immutable
public class EnforceSingleRowNode
    extends InternalPlanNode {

  private final PlanNode source;

  @JsonCreator
  public EnforceSingleRowNode(
      @JsonProperty("id") PlanNodeId id,
      @JsonProperty("source") PlanNode source) {
    super(id);

    this.source = requireNonNull(source, "source is null");
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  @Override
  public List<VariableReferenceExpression> getOutputVariables() {
    return source.getOutputVariables();
  }

  @JsonProperty("source")
  public PlanNode getSource() {
    return source;
  }

  @Override
  public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context) {
    return visitor.visitEnforceSingleRow(this, context);
  }

  @Override
  public PlanNode replaceChildren(List<PlanNode> newChildren) {
    return new EnforceSingleRowNode(getId(), Iterables.getOnlyElement(newChildren));
  }
}
