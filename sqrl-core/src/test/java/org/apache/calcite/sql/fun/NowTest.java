package org.apache.calcite.sql.fun;

import static org.junit.jupiter.api.Assertions.assertEquals;

import ai.datasqrl.plan.calcite.SqrlTypeFactory;
import ai.datasqrl.plan.calcite.SqrlTypeSystem;
import java.util.ArrayList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class NowTest {

  @Test
  public void test() {
    Now now = new Now();
    SqrlTypeFactory typeFactory = new SqrlTypeFactory(new SqrlTypeSystem());
    RelDataType type = now.inferReturnType(typeFactory, new ArrayList<>());
    assertEquals(type, typeFactory.createSqlType(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE, 0));
  }
}