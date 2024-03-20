package com.datasqrl.engine.kafka;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestNameSanitation {

  @Test
  public void testSanitize() {
    assertEquals("test-5",KafkaLogEngine.sanitizeName("test$5"));
    assertEquals("test-5-5",KafkaLogEngine.sanitizeName("test$5$5"));
    assertEquals("te-st-5.4",KafkaLogEngine.sanitizeName("te$st$5.4"));
  }

}
