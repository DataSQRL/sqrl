package com.datasqrl.engine.stream.flink.sql.model;

import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

@Getter
public class StreamPipelineItem extends QueryPipelineItem {

  public StreamPipelineItem(String namePrefix,
      SqlNode node, String sql, RelNode relNode, int uniqueIndex) {
    super(namePrefix, node, sql, relNode, uniqueIndex);
  }
}
