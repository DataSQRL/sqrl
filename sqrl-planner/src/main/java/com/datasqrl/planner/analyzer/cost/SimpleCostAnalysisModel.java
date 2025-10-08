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

import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.database.AnalyticDatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.analyzer.cost.ComputeCost.Simple;
import java.util.List;
import lombok.NonNull;

public record SimpleCostAnalysisModel(@NonNull Type type) implements CostModel {

  public enum Type {
    DEFAULT, // Favors processing data at ingestion time unless the operation is too expensive (e.g.
             // inner join)
    READ, // Favors processing data at query time
    WRITE // Favors processing data at ingestion time
  }

  public Simple getSourceSinkCost() {
    return new Simple(0.0);
  }

  public Simple getCost(ExecutionStage executionStage, TableAnalysis tableAnalysis) {
    var cost = 1.0;
    switch (executionStage.engine().getType()) {
      case DATABASE:
        // Currently we make the simplifying assumption that database execution is the baseline and
        // we compare
        // other engines against it
        // However, if the database is a table format, we apply a penalty because query engines are
        // less efficient.
        if (executionStage.engine() instanceof AnalyticDatabaseEngine) {
          cost = cost * 1.3;
        }
        break;
      case STREAMS:
        cost =
            switch (type) {
                // We assume that pre-computing is generally cheaper (by factor of 10) unless
                // (standard) joins are involved which can lead to combinatorial explosion.
                // So, we primarily cost the joins
              case DEFAULT -> joinCost(tableAnalysis.getCosts()) / 10;
              case WRITE -> cost / 10; // Make it always cheaper than database
              case READ -> cost * 2; // Make it more expensive than database to favor reads
            };
        break;
      case SERVER:
        cost = cost * 4;
        break;
      case LOG:
        cost = cost * 1.5;
        // We give logs that don't support mutations a slight benefit since they were added to
        // support subscriptions like this
        if (executionStage.supportsFeature(EngineFeature.MUTATIONS)) {
          cost = cost * 1.2;
        }
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported engine type: " + executionStage.engine().getType());
    }
    return new Simple(cost);
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
