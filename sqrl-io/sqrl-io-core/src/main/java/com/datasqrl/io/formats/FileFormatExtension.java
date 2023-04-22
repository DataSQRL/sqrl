/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.formats;

import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

public class FileFormatExtension {

  public static boolean matches(FormatFactory factory, String format) {
    if (Strings.isNullOrEmpty(format)) {
      return false;
    }
    return factory.getExtensions().contains(normalizeFormat(format));
  }

  public static boolean validFormat(String format) {
    return getFormat(format).isPresent();
  }

  public static Optional<FormatFactory> getFormat(String format) {
    String normFormat = normalizeFormat(format);
    return ServiceLoaderDiscovery.findFirst(FormatFactory.class, ff -> ff.getExtensions().contains(normFormat));
  }

  private static String normalizeFormat(String format) {
    return format.trim().toLowerCase();
  }
}
