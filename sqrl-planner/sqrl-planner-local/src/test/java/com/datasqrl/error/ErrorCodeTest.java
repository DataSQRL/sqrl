package com.datasqrl.error;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorCodeTest {

  @Test
  public void testErrorMessages() {
    for (ErrorCode code : ErrorCode.values()) {
      Assertions.assertTrue(!code.getErrorDescription().isEmpty(), code.getLabel());
    }
  }

}
