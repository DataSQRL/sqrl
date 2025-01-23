package com.datasqrl.engine.stream.flink.plan;

import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.sql.parser.ddl.SqlCreateFunction;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.dml.RichSqlInsert;

@lombok.Value
public class FlinkSqlResult {

  private Set<SqlCall> sinksSources;
  private List<SqlCall> stubSinksSources;
  private List<RichSqlInsert> inserts;
  private List<SqlCreateView> queries;
  private List<SqlCreateFunction> functions;

  public List<SqlNode> getStubSchema() {
    return ListUtils.union(functions, stubSinksSources);
  }
}
