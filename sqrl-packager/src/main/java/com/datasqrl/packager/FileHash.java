package com.datasqrl.packager;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileHash {

  public static String getFor(Path path) throws IOException {
    try (InputStream is = Files.newInputStream(path)) {
      return getFor(is);
    }
  }

  public static String getFor(InputStream is) throws IOException {
    return org.apache.commons.codec.digest.DigestUtils.md5Hex(is);
  }

}
