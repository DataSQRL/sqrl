package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode;
import com.datasqrl.engine.stream.flink.sql.model.QueryPipelineItem;
import com.google.auto.service.AutoService;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

@AutoService(RelToSqlNode.class)
public class FlinkRelToSqlNode implements RelToSqlNode {

  @Getter AtomicInteger atomicInteger = new AtomicInteger();

  @Override
  public FlinkSqlNodes convert(RelNode relNode) {
    FlinkRelToSqlConverter relToSqlConverter = new FlinkRelToSqlConverter(atomicInteger);
    SqlNode sqlNode = relToSqlConverter.visitRoot(relNode).asStatement();

    return new FlinkSqlNodes(sqlNode, relToSqlConverter.getQueries());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.FLINK;
  }

  @Value
  public static class FlinkSqlNodes implements SqlNodes {

    SqlNode sqlNode;
    List<QueryPipelineItem> queryList;
  }
}
