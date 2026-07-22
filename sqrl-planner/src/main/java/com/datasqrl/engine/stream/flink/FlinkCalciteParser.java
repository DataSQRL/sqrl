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
package com.datasqrl.engine.stream.flink;

import static com.google.common.base.Preconditions.checkArgument;

import jakarta.annotation.Nullable;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.parse.CalciteParser;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class FlinkCalciteParser {

  public static SqlNode parseSql(String sqlStmt) {
    return parseSql(sqlStmt, null);
  }

  public static SqlNode parseSql(String sqlStmt, @Nullable TableEnvironmentImpl tEnv) {
    var parser = getCalciteParser(Optional.ofNullable(tEnv));

    var sqlNodeList = parser.parseSqlList(sqlStmt);
    var parsed = sqlNodeList.getList();
    checkArgument(
        parsed.size() == 1,
        "Expected exactly 1 SQL statement but found %s. SQL: [%s]",
        parsed.size(),
        sqlStmt.length() > 500 ? sqlStmt.substring(0, 500) + "..." : sqlStmt);

    return parsed.get(0);
  }

  private static CalciteParser getCalciteParser(Optional<TableEnvironmentImpl> tEnv) {
    var tEnvImpl = tEnv.orElseGet(() -> TableEnvironmentImpl.create(new Configuration()));

    return ((PlannerBase) tEnvImpl.getPlanner()).createFlinkPlanner().parser();
  }
}
