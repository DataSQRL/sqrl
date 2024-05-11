package com.datasqrl;

import org.junit.jupiter.api.Test;

class FlinkMainTest {


  @Test
  public void testFlinkMain() {
    FlinkMain flinkMain = new FlinkMain();
    try {
      flinkMain.run().print();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}