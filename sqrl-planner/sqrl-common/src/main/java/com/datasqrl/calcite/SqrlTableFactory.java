package com.datasqrl.calcite;

import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.util.List;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqrlTableFunctionDef;

public interface SqrlTableFactory {

  void createTable(List<String> path, RelNode input, List<RelHint> hints,
      boolean setFieldNames, Optional<SqlNodeList> opHints, SqrlTableFunctionDef args);
}
