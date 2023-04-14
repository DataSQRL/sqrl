package com.datasqrl.engine.stream.flink.sql.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;


@Getter
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
@AllArgsConstructor
public class QueryPipelineItem implements ExecutionPipelineItem {

  String tableName;
  @JsonIgnore
  SqlNode node;
  String sql;
  @JsonIgnore
  RelNode relNode;

  public QueryPipelineItem(String namePrefix,
      SqlNode node, String sql, RelNode relNode, int uniqueIndex) {
    this.node = node;
    this.tableName = namePrefix + "$" + uniqueIndex;
    this.sql = sql;
    this.relNode = relNode;
  }
}
