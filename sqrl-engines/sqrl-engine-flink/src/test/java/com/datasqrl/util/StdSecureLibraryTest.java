package com.datasqrl.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.SecureFunctions;
import org.junit.jupiter.api.Test;

public class StdSecureLibraryTest {

  @Test
  public void testRandomId() {
    assertEquals(27, SecureFunctions.RANDOM_ID.eval(20).length());
  }

}
