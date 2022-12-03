package com.datasqrl.plan.calcite.util;

import org.apache.calcite.sql.SqlOperator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SqrlRexUtilTest {

  @Test
  public void testLookupOp() {
    SqlOperator op = SqrlRexUtil.getSqrlOperator("NOW");
    assertNotNull(op);
    assertEquals("NOW", op.getName());
  }
}