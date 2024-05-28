package com.datasqrl;

import org.junit.jupiter.api.Test;

class InitIcebergTest {


  @Test
  public void testFlinkMain() {
    InitIceberg initIceberg = new InitIceberg();
    try {
      initIceberg.run();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}