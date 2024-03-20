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

  public QueryPipelineItem(SqlNode node, String tableName) {
    this.node = node;
    this.tableName = tableName;
  }
}
