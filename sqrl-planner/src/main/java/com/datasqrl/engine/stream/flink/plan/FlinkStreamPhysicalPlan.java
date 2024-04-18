/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.stream.flink.plan;

import com.datasqrl.engine.stream.StreamPhysicalPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableOption;

@Getter
public class FlinkStreamPhysicalPlan implements StreamPhysicalPlan {
  final StreamStagePlan plan;
  final List<String> flinkSql;
  private final Set<String> connectors;
  private final Set<String> formats;
  public FlinkStreamPhysicalPlan(StreamStagePlan plan, List<String> flinkSql,
      List<SqlNode> sqlNodes) {
    this.plan = plan;
    this.flinkSql = flinkSql;
    this.connectors = extractConnectors(sqlNodes);
    this.formats = extractFormats(sqlNodes);
  }

  private Set<String> extractConnectors(List<SqlNode> sqlNodes) {
    Set<String> connectors = new HashSet<>();
    for (SqlNode node : sqlNodes) {
      if (node instanceof SqlCreateTable) {
        for (SqlNode option : ((SqlCreateTable)node).getPropertyList().getList()){
          SqlTableOption sqlTableOption = (SqlTableOption)option;
          if (sqlTableOption.getKeyString().equalsIgnoreCase("connector")) {
            connectors.add( sqlTableOption.getValueString());
          }
        }
      }
    }
    return connectors;
  }

  private Set<String> extractFormats(List<SqlNode> sqlNodes) {
    Set<String> formats = new HashSet<>();
    for (SqlNode node : sqlNodes) {
      if (node instanceof SqlCreateTable) {
        for (SqlNode option : ((SqlCreateTable)node).getPropertyList().getList()){
          SqlTableOption sqlTableOption = (SqlTableOption)option;
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

  public static final String FLINK_PLAN_FILENAME = "flink-plan.json";

  public boolean containsConnector(String connector) {

    return false;
  }
}
