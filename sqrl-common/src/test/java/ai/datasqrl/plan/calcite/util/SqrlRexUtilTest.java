package ai.datasqrl.plan.calcite.util;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.sql.SqlOperator;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

class SqrlRexUtilTest {

  @Test
  public void testLookupOp() {
    SqlOperator op = SqrlRexUtil.getSqrlOperator("NOW");
    assertNotNull(op);
    assertEquals("NOW", op.getName());
  }
}