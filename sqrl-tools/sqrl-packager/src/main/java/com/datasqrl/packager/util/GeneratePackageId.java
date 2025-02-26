/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.util;

import java.security.SecureRandom;
import java.util.Base64;

public class GeneratePackageId {

  private static final SecureRandom random = new SecureRandom();
  private static final Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();

  public static String generate() {
    byte[] buffer = new byte[20];
    random.nextBytes(buffer);
    return encoder.encodeToString(buffer);
  }
}
