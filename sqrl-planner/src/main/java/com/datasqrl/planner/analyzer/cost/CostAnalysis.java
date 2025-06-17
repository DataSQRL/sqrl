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

/**
 * As we analyze a query, collect {@link CostAnalysis} as we encounter expensive operations that are
 * better to execute in certain engine types.
 *
 * <p>These are then analyzed collectively in the cost analysis model.
 */
public interface CostAnalysis {

  double getCostMultiplier();
}
