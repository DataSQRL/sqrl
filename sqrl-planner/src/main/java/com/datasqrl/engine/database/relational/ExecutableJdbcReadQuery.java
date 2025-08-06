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
package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.ExecutableQuery;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.graphql.jdbc.DatabaseType;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import lombok.Builder;
import lombok.Value;

/**
 * Represents a read query that can be executed via JDBC against a database. It has a SQL string for
 * the query which contains parameter references to the parameters of the associated {@link
 * com.datasqrl.planner.tables.SqrlTableFunction}.
 *
 * <p>Some JDBC engines don't support parameter references, i.e. the parameters are just a list in
 * the order that '?' occurs in the SQL string. In that case, we need to map those parameter
 * occurences to the function parameters which the optional parameterMap does.
 */
@Value
@Builder
public class ExecutableJdbcReadQuery implements ExecutableQuery {

  ExecutionStage stage;
  DatabaseType database;
  String sql;
  Duration cacheDuration;
  Optional<List<Integer>> parameterMap = Optional.empty(); // Not yet supported
}
