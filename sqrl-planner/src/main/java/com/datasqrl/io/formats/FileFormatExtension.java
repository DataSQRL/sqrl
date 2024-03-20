/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Strings;
import java.util.Optional;

public class FileFormatExtension {

  public static boolean matches(FormatFactoryOld factory, String format) {
    if (Strings.isNullOrEmpty(format)) {
      return false;
    }
    return factory.getExtensions().contains(normalizeFormat(format));
  }

  public static boolean validFormat(String format) {
    return getFormat(format).isPresent();
  }

  public static Optional<FormatFactoryOld> getFormat(String format) {
    String normFormat = normalizeFormat(format);
    return ServiceLoaderDiscovery.findFirst(FormatFactoryOld.class, ff -> ff.getExtensions().contains(normFormat));
  }

  private static String normalizeFormat(String format) {
    return format.trim().toLowerCase();
  }
}
