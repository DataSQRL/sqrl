/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
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
package com.datasqrl.plan.global;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.rules.EngineCapability;
import com.datasqrl.planner.analyzer.cost.ComputeCost;
import java.util.Collection;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Value;

@Getter
@AllArgsConstructor
public abstract class StageAnalysis {

  private final ExecutionStage stage;

  public abstract String getMessage();

  public boolean isSupported() {
    return false;
  }

  public String getName() {
    return stage.name();
  }

  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class Cost extends StageAnalysis {

    ComputeCost cost;
    boolean supported;

    public Cost(ExecutionStage stage, ComputeCost cost, boolean supported) {
      super(stage);
      this.cost = cost;
      this.supported = supported;
    }

    @Override
    public String getMessage() {
      if (supported) {
        return "Stage [%s] supported at cost: %s".formatted(getName(), cost);
      } else {
        return "Stage [%s] has been eliminated  due to high cost: %s".formatted(getName(), cost);
      }
    }

    @Override
    public boolean isSupported() {
      return supported;
    }

    public Cost tooExpensive() {
      return new Cost(super.getStage(), cost, false);
    }
  }

  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class MissingCapability extends StageAnalysis {

    Collection<EngineCapability> capabilities;

    public MissingCapability(ExecutionStage stage, Collection<EngineCapability> capabilities) {
      super(stage);
      this.capabilities = capabilities;
    }

    @Override
    public String getMessage() {
      return "Stage [%s] does not support capabilities: %s".formatted(getName(), capabilities);
    }
  }

  @Value
  @EqualsAndHashCode(callSuper = true)
  public static class MissingDependent extends StageAnalysis {

    boolean upstream; // else it's downstream
    String tableName;

    public MissingDependent(ExecutionStage stage, boolean upstream, String tableName) {
      super(stage);
      this.upstream = upstream;
      this.tableName = tableName;
    }

    @Override
    public String getMessage() {
      return "%s [%s] does not support stage [%s] or any %s stages"
          .formatted(
              upstream ? "Upstream input" : "Downstream consumer",
              tableName,
              getName(),
              upstream ? "prior" : "subsequent");
    }
  }
}
