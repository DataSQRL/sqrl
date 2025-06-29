package com.datasqrl.util;

import java.util.Optional;

public class EnumUtil {

  public static <T extends Enum<T>> Optional<T> getByName(Class<T> enumClass, String name) {
    try {
      return Optional.of(Enum.valueOf(enumClass, name.trim().toUpperCase()));
    } catch (IllegalArgumentException | NullPointerException e) {
      return Optional.empty();
    }
  }
}
