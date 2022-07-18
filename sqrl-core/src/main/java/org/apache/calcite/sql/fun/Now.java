package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.type.SqlTypeName;

public class Now extends SqlAbstractTimeFunction {

  public Now() {
    super("NOW", SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
  }
}
