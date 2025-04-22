/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class GeneratePackageIdTest {

  @Test
  public void generateIds() {
    Set<String> ids = new HashSet<>();
    for (var i = 0; i < 100000; i++) {
      var id = GeneratePackageId.generate();
      assertEquals(27, id.length());
      assertTrue(ids.add(id));
    }
  }

  @Test
  @Disabled
  public void generateSingleId() {
    System.out.println(GeneratePackageId.generate());
  }

}
