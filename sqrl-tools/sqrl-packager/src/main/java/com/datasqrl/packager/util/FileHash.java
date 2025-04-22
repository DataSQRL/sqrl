/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.packager.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHash {

  public static String getFor(Path path) throws IOException {
    try (var is = Files.newInputStream(path)) {
      return getFor(is);
    }
  }

  public static String getFor(InputStream is) throws IOException {
    return org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
  }

}
