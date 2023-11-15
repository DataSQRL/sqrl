package com.datasqrl.calcite;

import com.datasqrl.calcite.sql.DynamicParameterStrategy;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.ArrayList;
import java.util.List;

public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

  private final DynamicParameterStrategy parameterStrategy;

  public DynamicParamSqlPrettyWriter(SqlWriterConfig config, DynamicParameterStrategy parameterStrategy) {
    super(config);
    this.parameterStrategy = parameterStrategy;
  }

  @Override
  public void dynamicParam(int index) {
    parameterStrategy.apply(this, index);
    setNeedWhitespace(true);
  }

  public String unparse(SqlNode queryNode) {
    queryNode.unparse(this, 0, 0);
    return this.toSqlString().getSql();
  }
}