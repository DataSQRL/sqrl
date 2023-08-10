package com.datasqrl.calcite;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

public class SqrlSchema extends AbstractSchema {
  private  final Map<String, Table> tableMap = new HashMap<>();

  public void add(String tableName, Table table) {
    tableMap.put(tableName, table);
  }

  @Override
  protected Map<String, Table> getTableMap() {
    return tableMap;
  }
  // Take your own tables as well as table functions here
}
