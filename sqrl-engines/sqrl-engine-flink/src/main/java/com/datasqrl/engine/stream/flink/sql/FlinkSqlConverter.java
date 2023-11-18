package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.FlinkExecutablePlan.FlinkQuery;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlConverter;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.FlinkRelToSqlConverter;
import org.apache.calcite.sql.SqlNode;

@AutoService(SqlConverter.class)
public class FlinkSqlConverter implements SqlConverter {

  @Override
  public SqlNodes convert(RelNode relNode) {
    List<FlinkQuery> queries = new ArrayList<>();
    FlinkRelToSqlConverter relToSqlConverter = new FlinkRelToSqlConverter(queries);
    SqlNode sqlNode = RelToFlinkSql.convertToSqlNode(relToSqlConverter, relNode);

    return new FlinkSqlNodes(sqlNode, relToSqlConverter.getQueryList());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.FLINK;
  }

  @Value
  public static class FlinkSqlNodes implements SqlNodes {

    SqlNode sqlNode;
    List<FlinkQuery> queryList;
  }
}
