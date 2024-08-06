package com.datasqrl.engine.log.kafka;

import static com.datasqrl.engine.log.kafka.KafkaLogFactory.sanitizeName;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestNameSanitation {

  @Test
  public void testSanitize() {
    assertEquals("test-5", sanitizeName("test$5"));
    assertEquals("test-5-5",sanitizeName("test$5$5"));
    assertEquals("te-st-5.4",sanitizeName("te$st$5.4"));
  }

}
