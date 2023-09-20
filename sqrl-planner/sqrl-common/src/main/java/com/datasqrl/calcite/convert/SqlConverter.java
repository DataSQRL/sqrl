package com.datasqrl.calcite.convert;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;

//Service loader interface
public abstract class SqlConverter {

  public abstract SqlNode convert(RelNode relNode);
}
