package com.datasqrl.calcite;

import lombok.Getter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

import java.util.ArrayList;
import java.util.List;

public class DynamicParamSqlPrettyWriter extends SqlPrettyWriter {

  @Getter
  private List<Integer> dynamicParameters = new ArrayList<>();

  public DynamicParamSqlPrettyWriter(SqlWriterConfig config) {
    super(config);
  }

  @Override
  public void dynamicParam(int index) {
    if (dynamicParameters == null) {
      dynamicParameters = new ArrayList<>();
    }
    dynamicParameters.add(index);
    print("$" + (index + 1));
    setNeedWhitespace(true);
  }
}