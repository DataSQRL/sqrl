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
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.StreamPhysicalPlan;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;

@Getter
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {
  public static final String FLINK_PLAN_FILENAME = "flink-plan.json";

  final List<String> flinkSql;
  private final Set<String> connectors;
  private final Set<String> formats;

  public FlinkStreamPhysicalPlan(List<String> flinkSql, List<SqlNode> sqlNodes) {
    this.flinkSql = flinkSql;
    this.connectors = extractConnectors(sqlNodes);
    this.formats = extractFormats(sqlNodes);
  }

  private Set<String> extractConnectors(List<SqlNode> sqlNodes) {
    Set<String> connectors = new HashSet<>();
    for (SqlNode node : sqlNodes) {
      if (node instanceof SqlCreateTable table) {
        for (SqlNode option : table.getPropertyList().getList()) {
          var sqlTableOption = (SqlTableOption) option;
          if (sqlTableOption.getKeyString().equalsIgnoreCase("connector")) {
            connectors.add(sqlTableOption.getValueString());
          }
        }
      }
    }
    return connectors;
  }

  private Set<String> extractFormats(List<SqlNode> sqlNodes) {
    Set<String> formats = new HashSet<>();
    for (SqlNode node : sqlNodes) {
      if (node instanceof SqlCreateTable table) {
        for (SqlNode option : table.getPropertyList().getList()) {
          var sqlTableOption = (SqlTableOption) option;
          switch (sqlTableOption.getKeyString()) {
            case "format":
            case "key.format":
            case "value.format":
              formats.add(sqlTableOption.getValueString());
          }
        }
      }
    }
    return formats;
  }
}
