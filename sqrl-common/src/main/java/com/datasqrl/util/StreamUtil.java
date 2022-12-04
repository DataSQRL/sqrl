package com.datasqrl.util;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Stream;

public class StreamUtil {

  public static <T, C extends T> Stream<C> filterByClass(Stream<T> stream, Class<C> clazz) {
    return stream.filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T, C extends T> Stream<C> filterByClass(Collection<T> col, Class<C> clazz) {
    return col.stream().filter(clazz::isInstance).map(clazz::cast);
  }

  public static <T> Stream<T> getPresent(Stream<Optional<T>> stream) {
    return stream.filter(Optional::isPresent).map(Optional::get);
  }


}
