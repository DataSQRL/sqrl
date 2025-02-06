package com.datasqrl.functions.secure;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import com.datasqrl.secure.SecureFunctions;

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
