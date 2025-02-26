package com.datasqrl.calcite;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

  @Getter private final List<Integer> dynamicParameters = new ArrayList<>();

  public DynamicParamSqlPrettyWriter(SqlWriterConfig config) {
    super(config);
  }

  @Override
  public void dynamicParam(int index) {
    dynamicParameters.add(index);
    print("$" + (index + 1));
    setNeedWhitespace(true);
  }
}
