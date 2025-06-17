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
package com.datasqrl.planner.dag.plan;

import com.datasqrl.graphql.server.MutationComputedColumnType;
import lombok.Value;

/**
 * The name of the uuid and timestamp column (if any) as defined in the CREATE TABLE statement.
 *
 * <p>TODO: We want to generalize this to allow arbitrary upfront computations on the server and not
 * have this hardcoded to just these two columns.
 */
@Value
public class MutationComputedColumn {

  public static final String UUID_METADATA = "uuid";
  public static final String TIMESTAMP_METADATA = "timestamp";

  String columnName;
  MutationComputedColumnType type;
}
