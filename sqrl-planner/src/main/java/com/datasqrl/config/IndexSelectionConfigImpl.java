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
package com.datasqrl.config;

import lombok.AllArgsConstructor;

/** The {@code index-selection} object of a database engine configuration. */
@AllArgsConstructor
public class IndexSelectionConfigImpl implements PackageJson.IndexSelectionConfig {

  public static final String INDEX_SELECTION_KEY = "index-selection";
  public static final String MAX_INDEXES_KEY = "max-indexes";
  public static final String MAX_INDEX_COLUMNS_KEY = "max-index-columns";
  public static final String MAX_FILTER_PATTERNS_KEY = "max-filter-patterns";
  public static final String MIN_COST_REDUCTION_KEY = "min-cost-reduction";
  public static final String SMALL_TABLE_ROW_LIMIT_KEY = "small-table-row-limit";

  private final SqrlConfig sqrlConfig;

  @Override
  public int getMaxIndexes() {
    return sqrlConfig.asInt(MAX_INDEXES_KEY).validate(v -> v > 0, "Must be at least 1.").get();
  }

  @Override
  public int getMaxIndexColumns() {
    return sqrlConfig
        .asInt(MAX_INDEX_COLUMNS_KEY)
        .validate(v -> v > 0, "Must be at least 1.")
        .get();
  }

  @Override
  public int getMaxFilterPatterns() {
    return sqrlConfig
        .asInt(MAX_FILTER_PATTERNS_KEY)
        .validate(v -> v > 0, "Must be at least 1.")
        .get();
  }

  @Override
  public double getMinCostReduction() {
    return sqrlConfig
        .as(MIN_COST_REDUCTION_KEY, Double.class)
        .validate(v -> v >= 0 && v < 1, "Must be at least 0 and less than 1.")
        .get();
  }

  @Override
  public int getSmallTableRowLimit() {
    return sqrlConfig
        .asInt(SMALL_TABLE_ROW_LIMIT_KEY)
        .validate(v -> v >= 0, "Must be at least 0.")
        .get();
  }
}
