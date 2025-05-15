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
package com.datasqrl.planner.analyzer.cost;

import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.rules.ComputeCost;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.google.common.base.Preconditions;
import java.util.List;
import lombok.NonNull;
import lombok.Value;

@Value
public class SimpleCostAnalysisModel implements ComputeCost {

  private final double cost;

  private SimpleCostAnalysisModel(double cost) {
    this.cost = cost;
  }

  public static SimpleCostAnalysisModel ofSourceSink() {
    return new SimpleCostAnalysisModel(0.0);
  }

  public static SimpleCostAnalysisModel of(
      ExecutionStage executionStage, TableAnalysis tableAnalysis) {
    var cost = 1.0;
    switch (executionStage.getEngine().getType()) {
      case DATABASE:
        // Currently we make the simplifying assumption that database execution is the baseline and
        // we compare
        // other engines against it
        // However, if the database is a table format, we apply a penalty because query engines are
        // less efficient.
        if (executionStage.getEngine() instanceof AnalyticDatabaseEngine) {
          cost = cost * 1.3;
        }
        break;
      case STREAMS:
        // We assume that pre-computing is generally cheaper (by factor of 10) unless (standard)
        // joins are
        // involved which can lead to combinatorial explosion. So, we primarily cost the joins
        cost = joinCost(tableAnalysis.getCosts());
        cost = cost / 10;
        break;
      case SERVER:
        cost = cost * 2;
        break;
      case LOG:
        cost = cost * 1.5;
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported engine type: " + executionStage.getEngine().getType());
    }
    return new SimpleCostAnalysisModel(cost);
  }

  @Override
  public int compareTo(@NonNull ComputeCost o) {
    Preconditions.checkArgument(o instanceof SimpleCostAnalysisModel);
    return Double.compare(cost, ((SimpleCostAnalysisModel) o).cost);
  }

  public static double joinCost(List<CostAnalysis> costs) {
    var joinCost = 1.0;
    for (CostAnalysis costAnalysis : costs) {
      if (costAnalysis instanceof JoinCostAnalysis) {
        joinCost *= costAnalysis.getCostMultiplier();
      }
    }
    return joinCost;
  }
}
