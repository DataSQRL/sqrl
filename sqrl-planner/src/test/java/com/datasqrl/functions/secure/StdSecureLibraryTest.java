package com.datasqrl.functions.secure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.secure.SecureFunctions;
import org.junit.jupiter.api.Test;

public class StdSecureLibraryTest {

  @Test
  public void testRandomId() {
    assertEquals(27, SecureFunctions.RANDOM_ID.eval(20L).length());
  }

  @Test
  public void testUuid() {
    assertEquals(36, SecureFunctions.UUID.eval().length());
  }
}
